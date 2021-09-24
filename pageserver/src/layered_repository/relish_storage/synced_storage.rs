use std::{
    collections::{BTreeSet, BinaryHeap, HashMap},
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc, Mutex},
    thread,
    time::Duration,
};

use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;
use zenith_utils::{
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

use crate::{
    layered_repository::{
        delta_layer::DeltaLayer,
        filename::{DeltaFileName, ImageFileName, PathOrConf},
        image_layer::ImageLayer,
        metadata_path,
        relish_storage::RelishKind,
    },
    PageServerConf,
};

use super::{local_fs::LocalFs, RelishStorage};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SyncTask {
    UrgentDownload(RemoteTimeline),
    Upload(LocalTimeline),
    Download(RemoteTimeline),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalTimeline {
    pub tenant_id: ZTenantId,
    pub timeline_id: ZTimelineId,
    pub disk_consistent_lsn: Lsn,
    pub metadata_path: PathBuf,
    pub image_layers: BTreeSet<ImageFileName>,
    pub delta_layers: BTreeSet<DeltaFileName>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RemoteTimeline {
    pub tenant_id: ZTenantId,
    pub timeline_id: ZTimelineId,
    pub has_metadata: bool,
    pub image_layers: BTreeSet<ImageFileName>,
    pub delta_layers: BTreeSet<DeltaFileName>,
}

// TODO kb shared channel instead?
lazy_static::lazy_static! {
    pub static ref RELISH_STORAGE_WITH_BACKGROUND_SYNC: Arc<RelishStorageWithBackgroundSync> = Arc::new(RelishStorageWithBackgroundSync::new());
}

pub struct RelishStorageWithBackgroundSync {
    enabled: AtomicBool,
    queue: Mutex<BinaryHeap<SyncTask>>,
}

impl RelishStorageWithBackgroundSync {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            queue: Mutex::new(BinaryHeap::new()),
        }
    }

    pub fn schedule_timeline_upload(&self, timeline_upload: LocalTimeline) {
        if self.is_enabled() {
            self.queue
                .lock()
                .unwrap()
                .push(SyncTask::Upload(timeline_upload));
        }
    }

    fn schedule_timeline_download(&self, timeline_download: RemoteTimeline, urgent: bool) {
        if self.is_enabled() {
            self.queue.lock().unwrap().push(if urgent {
                SyncTask::UrgentDownload(timeline_download)
            } else {
                SyncTask::Download(timeline_download)
            })
        }
    }

    fn disable(&self) {
        self.enabled
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.queue.lock().unwrap().clear();
    }

    fn is_enabled(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn next(&self) -> Option<SyncTask> {
        if self.is_enabled() {
            let mut queue_accessor = self.queue.lock().unwrap();
            let new_task = queue_accessor.pop();
            log::debug!("current storage queue length: {}", queue_accessor.len());
            new_task
        } else {
            None
        }
    }
}

pub fn create_storage_sync_thread(
    config: &'static PageServerConf,
) -> anyhow::Result<Option<thread::JoinHandle<()>>> {
    // TODO kb revert
    // match &config.relish_storage_config {
    //     Some(RelishStorageConfig::LocalFs(root)) => {
    //         let relish_storage = LocalFs::new(root.clone())?;
    //         Ok(Some(run_thread(
    //             Arc::clone(&RELISH_STORAGE_WITH_BACKGROUND_SYNC),
    //             relish_storage,
    //             &config.workdir,
    //         )?))
    //     }
    //     Some(RelishStorageConfig::AwsS3(s3_config)) => {
    //         let relish_storage = RustS3::new(s3_config)?;
    //         Ok(Some(run_thread(
    //             Arc::clone(&RELISH_STORAGE_WITH_BACKGROUND_SYNC),
    //             relish_storage,
    //             &config.workdir,
    //         )?))
    //     }
    //     None => {
    //         RELISH_STORAGE_WITH_BACKGROUND_SYNC.disable();
    //         Ok(None)
    //     }
    // }
    let relish_storage = LocalFs::new(PathBuf::from("/Users/someonetoignore/Downloads/tmp_dir"))?;
    Ok(Some(run_thread(
        config,
        Arc::clone(&RELISH_STORAGE_WITH_BACKGROUND_SYNC),
        relish_storage,
    )?))
}

fn run_thread<P: std::fmt::Debug, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    sync_tasks_queue: Arc<RelishStorageWithBackgroundSync>,
    relish_storage: S,
) -> std::io::Result<thread::JoinHandle<()>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    thread::Builder::new()
        .name("Queue based relish storage sync".to_string())
        .spawn(move || {
            // TODO kb determine the latest LSN timeline to download now, add all non-downloaded files to the queue
            let mut remote_timelines = categorize_relish_uploads::<P, S>(
                runtime
                    .block_on(relish_storage.list_relishes())
                    .expect("Failed to list relish uploads"),
            );
            // Now think of how Vec<P> is mapped against TimelineUpload data (we need to determine that the upload happened)
            // (need to parse the uploaded paths at least)
            // let mut uploads: HashMap<(ZTenantId, ZTimelineId), BTreeSet<Lsn>>
            // downloads should go straight to queue
            // let mut files_to_download: Vec<P>
            loop {
                match sync_tasks_queue.next() {
                    Some(task) => runtime.block_on(async {
                        match task {
                            SyncTask::Download(download_data) => {
                                download_timeline(
                                    config,
                                    &sync_tasks_queue,
                                    &relish_storage,
                                    download_data,
                                    false,
                                )
                                .await
                            }
                            SyncTask::UrgentDownload(download_data) => {
                                download_timeline(
                                    config,
                                    &sync_tasks_queue,
                                    &relish_storage,
                                    download_data,
                                    true,
                                )
                                .await
                            }
                            SyncTask::Upload(layer_upload) => {
                                upload_timeline(
                                    config,
                                    &mut remote_timelines,
                                    &sync_tasks_queue,
                                    &relish_storage,
                                    layer_upload,
                                )
                                .await
                            }
                        }
                    }),
                    None => {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                };
            }
        })
}

fn categorize_relish_uploads<
    P: std::fmt::Debug,
    S: 'static + RelishStorage<RelishStoragePath = P>,
>(
    uploaded_relishes: Vec<P>,
) -> HashMap<(ZTenantId, ZTimelineId), RemoteTimeline> {
    let mut timelines = HashMap::new();

    for relish_path in uploaded_relishes {
        match S::info(&relish_path) {
            Ok(relish_info) => {
                let tenant_id = relish_info.tenant_id;
                let timeline_id = relish_info.timeline_id;
                let timeline_files =
                    timelines
                        .entry((tenant_id, timeline_id))
                        .or_insert_with(|| RemoteTimeline {
                            image_layers: BTreeSet::new(),
                            delta_layers: BTreeSet::new(),
                            has_metadata: false,
                            tenant_id,
                            timeline_id,
                        });

                match relish_info.kind {
                    RelishKind::Metadata => {
                        timeline_files.has_metadata = true;
                    }
                    RelishKind::DeltaRelish(delta_relish) => {
                        timeline_files.delta_layers.insert(delta_relish);
                    }
                    RelishKind::ImageRelish(image_relish) => {
                        timeline_files.image_layers.insert(image_relish);
                    }
                }
            }
            Err(e) => {
                log::error!(
                    "Failed to get relish info from the path '{:?}', reason: {}",
                    relish_path,
                    e
                );
                continue;
            }
        }
    }

    timelines
}

// TODO kb very much duplicates the corresponding upload counterpart
async fn download_timeline<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    sync_tasks_queue: &'a RelishStorageWithBackgroundSync,
    relish_storage: &'a S,
    remote_timeline: RemoteTimeline,
    urgent: bool,
) {
    let timeline_id = remote_timeline.timeline_id;
    let tenant_id = remote_timeline.tenant_id;
    log::debug!("Downloading layers for timeline {}", timeline_id);

    if !remote_timeline.has_metadata {
        log::debug!("Remote timeline incomplete and has no metadata file, aborting the download");
        return;
    }
    let metadata_path = metadata_path(config, timeline_id, tenant_id);
    if metadata_path.exists() {
        log::debug!("Metadata file is already present locally, aborting download");
        return;
    }

    log::debug!(
        "Downloading {} image and {} delta layers",
        remote_timeline.image_layers.len(),
        remote_timeline.delta_layers.len(),
    );

    // TODO kb put into config
    let concurrent_download_limit = Arc::new(Semaphore::new(10));
    let mut relish_downloads = FuturesUnordered::new();

    for download in remote_timeline
        .image_layers
        .into_iter()
        .map(Layer::Image)
        .chain(remote_timeline.delta_layers.into_iter().map(Layer::Delta))
    {
        let download_limit = Arc::clone(&concurrent_download_limit);
        relish_downloads.push(async move {
            let conf = PathOrConf::Conf(config);
            let relish_local_path = match &download {
                Layer::Image(image_name) => {
                    ImageLayer::path_for(&conf, timeline_id, tenant_id, image_name)
                }
                Layer::Delta(delta_name) => {
                    DeltaLayer::path_for(&conf, timeline_id, tenant_id, delta_name)
                }
            };
            let permit = download_limit
                .acquire()
                .await
                .expect("Semaphore is not closed yet");
            let download_result =
                download_relish(relish_storage, &config.workdir, &relish_local_path).await;
            drop(permit);
            (download, relish_local_path, download_result)
        });
    }

    let mut failed_image_downloads = BTreeSet::new();
    let mut failed_delta_downloads = BTreeSet::new();
    while let Some((download, relish_local_path, relish_download_result)) =
        relish_downloads.next().await
    {
        match relish_download_result {
            Ok(()) => {
                log::trace!(
                    "Successfully downloaded relish '{}'",
                    relish_local_path.display()
                );
            }
            Err(e) => {
                log::error!(
                    "Failed to download relish '{}', reason: {}",
                    relish_local_path.display(),
                    e
                );
                match download {
                    Layer::Image(image_name) => {
                        failed_image_downloads.insert(image_name);
                    }
                    Layer::Delta(delta_name) => {
                        failed_delta_downloads.insert(delta_name);
                    }
                }
            }
        }
    }
    concurrent_download_limit.close();

    if failed_image_downloads.is_empty() && failed_delta_downloads.is_empty() {
        log::debug!("Successfully downloaded all relishes");

        match download_relish(relish_storage, &config.workdir, &metadata_path).await {
            // TODO kb how to load that into pageserver now?
            Ok(()) => log::debug!("Successfully downloaded the metadata file"),
            Err(e) => {
                log::error!(
                    "Failed to download metadata file '{}', reason: {}",
                    metadata_path.display(),
                    e
                );
                sync_tasks_queue.schedule_timeline_download(
                    RemoteTimeline {
                        image_layers: BTreeSet::new(),
                        delta_layers: BTreeSet::new(),
                        ..remote_timeline
                    },
                    urgent,
                );
            }
        }
    } else {
        log::error!(
            "Failed to download {} image layers and {} delta layers, rescheduling the job",
            failed_image_downloads.len(),
            failed_delta_downloads.len(),
        );
        sync_tasks_queue.schedule_timeline_download(
            RemoteTimeline {
                image_layers: failed_image_downloads,
                delta_layers: failed_delta_downloads,
                ..remote_timeline
            },
            urgent,
        );
    }
}

async fn download_relish<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    relish_storage: &S,
    page_server_workdir: &Path,
    relish_local_path: &Path,
) -> anyhow::Result<()> {
    if relish_local_path.exists() {
        Ok(())
    } else {
        let storage_path =
            S::storage_path(page_server_workdir, relish_local_path).with_context(|| {
                format!(
                    "Failed to derive storage destination out of metadata path {}",
                    relish_local_path.display()
                )
            })?;
        relish_storage
            .download_relish(&storage_path, relish_local_path)
            .await
    }
}

enum Layer {
    Image(ImageFileName),
    Delta(DeltaFileName),
}

async fn upload_timeline<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    remote_timelines: &'a mut HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>,
    sync_tasks_queue: &'a RelishStorageWithBackgroundSync,
    relish_storage: &'a S,
    mut new_upload: LocalTimeline,
) {
    let tenant_id = new_upload.tenant_id;
    let timeline_id = new_upload.timeline_id;
    log::debug!("Uploading layers for timeline {}", timeline_id);

    let uploaded_files = remote_timelines.get(&(tenant_id, timeline_id));
    if let Some(uploaded_timeline_files) = uploaded_files {
        new_upload.image_layers.retain(|path_to_upload| {
            !uploaded_timeline_files
                .image_layers
                .contains(path_to_upload)
        });
        new_upload.delta_layers.retain(|path_to_upload| {
            !uploaded_timeline_files
                .delta_layers
                .contains(path_to_upload)
        });
        if new_upload.image_layers.is_empty()
            && new_upload.delta_layers.is_empty()
            && uploaded_timeline_files.has_metadata
        {
            log::debug!("All layers are uploaded already");
            return;
        }
    }

    // TODO kb put into config
    let concurrent_upload_limit = Arc::new(Semaphore::new(10));
    let mut relish_uploads = FuturesUnordered::new();

    for upload in new_upload
        .image_layers
        .into_iter()
        .map(Layer::Image)
        .chain(new_upload.delta_layers.into_iter().map(Layer::Delta))
    {
        let upload_limit = Arc::clone(&concurrent_upload_limit);
        relish_uploads.push(async move {
            let conf = PathOrConf::Conf(config);
            let relish_local_path = match &upload {
                Layer::Image(image_name) => {
                    ImageLayer::path_for(&conf, timeline_id, tenant_id, image_name)
                }
                Layer::Delta(delta_name) => {
                    DeltaLayer::path_for(&conf, timeline_id, tenant_id, delta_name)
                }
            };
            let permit = upload_limit
                .acquire()
                .await
                .expect("Semaphore is not closed yet");
            let upload_result =
                upload_relish(relish_storage, &config.workdir, &relish_local_path).await;
            drop(permit);
            (upload, relish_local_path, upload_result)
        });
    }

    let mut failed_image_uploads = BTreeSet::new();
    let mut failed_delta_uploads = BTreeSet::new();
    let mut successful_image_uploads = BTreeSet::new();
    let mut successful_delta_uploads = BTreeSet::new();
    while let Some((upload, relish_local_path, relish_upload_result)) = relish_uploads.next().await
    {
        match relish_upload_result {
            Ok(()) => {
                log::trace!(
                    "Successfully uploaded relish '{}'",
                    relish_local_path.display()
                );
                match upload {
                    Layer::Image(image_name) => {
                        successful_image_uploads.insert(image_name);
                    }
                    Layer::Delta(delta_name) => {
                        successful_delta_uploads.insert(delta_name);
                    }
                }
            }
            Err(e) => {
                log::error!(
                    "Failed to upload relish '{}', reason: {}",
                    relish_local_path.display(),
                    e
                );
                match upload {
                    Layer::Image(image_name) => {
                        failed_image_uploads.insert(image_name);
                    }
                    Layer::Delta(delta_name) => {
                        failed_delta_uploads.insert(delta_name);
                    }
                }
            }
        }
    }
    concurrent_upload_limit.close();

    if failed_image_uploads.is_empty() && failed_delta_uploads.is_empty() {
        log::debug!("Successfully uploaded all relishes");

        match upload_relish(relish_storage, &config.workdir, &new_upload.metadata_path).await {
            Ok(()) => {
                log::debug!("Successfully uploaded the metadata file");
                let entry_to_update = remote_timelines
                    .entry((tenant_id, timeline_id))
                    .or_insert_with(|| RemoteTimeline {
                        image_layers: BTreeSet::new(),
                        delta_layers: BTreeSet::new(),
                        has_metadata: true,
                        tenant_id,
                        timeline_id,
                    });

                entry_to_update
                    .image_layers
                    .extend(successful_image_uploads.into_iter());
                entry_to_update
                    .delta_layers
                    .extend(successful_delta_uploads.into_iter());
            }
            Err(e) => {
                log::error!(
                    "Failed to upload metadata file '{}', reason: {}",
                    new_upload.metadata_path.display(),
                    e
                );
                sync_tasks_queue.schedule_timeline_upload(LocalTimeline {
                    image_layers: BTreeSet::new(),
                    delta_layers: BTreeSet::new(),
                    ..new_upload
                });
            }
        }
    } else {
        log::error!(
            "Failed to upload {} image layers and {} delta layers, rescheduling the job",
            failed_image_uploads.len(),
            failed_delta_uploads.len(),
        );
        sync_tasks_queue.schedule_timeline_upload(LocalTimeline {
            image_layers: failed_image_uploads,
            delta_layers: failed_delta_uploads,
            ..new_upload
        });
    }
}

async fn upload_relish<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    relish_storage: &S,
    page_server_workdir: &Path,
    relish_local_path: &Path,
) -> anyhow::Result<()> {
    let destination =
        S::storage_path(page_server_workdir, &relish_local_path).with_context(|| {
            format!(
                "Failed to derive storage destination out of metadata path {}",
                relish_local_path.display()
            )
        })?;
    relish_storage
        .upload_relish(&relish_local_path, &destination)
        .await
}
