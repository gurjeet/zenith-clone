use std::{
    collections::{BTreeSet, BinaryHeap, HashMap},
    path::Path,
    sync::{Arc, Mutex},
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

use crate::layered_repository::LocalTimeline;
use crate::{
    layered_repository::{
        delta_layer::DeltaLayer,
        filename::{DeltaFileName, ImageFileName, PathOrConf},
        image_layer::ImageLayer,
        metadata_path,
    },
    relish_storage::RelishKind,
    PageServerConf,
};

use super::RelishStorage;

lazy_static::lazy_static! {
    static ref UPLOAD_QUEUE: Arc<Mutex<BinaryHeap<SyncTask>>> =
        Arc::new(Mutex::new(BinaryHeap::new()));
}

pub struct StorageUploader {}

impl StorageUploader {
    pub fn upload_relish(&self, local_timeline: LocalTimeline) {
        UPLOAD_QUEUE
            .lock()
            .unwrap()
            .push(SyncTask::Upload(local_timeline))
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SyncTask {
    UrgentDownload(RemoteTimeline),
    Upload(LocalTimeline),
    Download(RemoteTimeline),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RemoteTimeline {
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    has_metadata: bool,
    image_layers: BTreeSet<ImageFileName>,
    delta_layers: BTreeSet<DeltaFileName>,
}

pub fn run_storage_sync_thread<
    P: std::fmt::Debug,
    S: 'static + RelishStorage<RelishStoragePath = P>,
>(
    config: &'static PageServerConf,
    relish_storage: S,
) -> anyhow::Result<Option<(StorageUploader, thread::JoinHandle<anyhow::Result<()>>)>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let handle = thread::Builder::new()
        .name("Queue based relish storage sync".to_string())
        .spawn(move || {
            let mut remote_timelines = categorize_relish_uploads::<P, S>(
                runtime
                    .block_on(relish_storage.list_relishes())
                    .expect("Failed to list relish uploads"),
            );
            let latest_tenant_timelines = latest_timelines_for_tenants(&remote_timelines);
            // TODO kb download & load the timelines into the pageserver
            log::warn!("@@@@@@@@@@@, {:?}", latest_tenant_timelines);

            loop {
                let mut queue_accessor = UPLOAD_QUEUE.lock().unwrap();
                log::debug!("Upload queue length: {}", queue_accessor.len());
                let next_task = queue_accessor.pop();
                drop(queue_accessor);
                match next_task {
                    Some(task) => runtime.block_on(async {
                        match task {
                            SyncTask::Download(download_data) => {
                                download_timeline(config, &relish_storage, download_data, false)
                                    .await
                            }
                            SyncTask::UrgentDownload(download_data) => {
                                download_timeline(config, &relish_storage, download_data, true)
                                    .await
                            }
                            SyncTask::Upload(layer_upload) => {
                                upload_timeline(
                                    config,
                                    &mut remote_timelines,
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
        })?;
    Ok(Some((StorageUploader {}, handle)))
}

fn latest_timelines_for_tenants(
    remote_timelines: &HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>,
) -> HashMap<ZTenantId, ZTimelineId> {
    let mut latest_timelines_for_tenants = HashMap::with_capacity(remote_timelines.len());

    for ((remote_tenant_id, remote_timeline_id), remote_timeline_data) in remote_timelines {
        let (latest_timeline_id, timeline_latest_lsn) = latest_timelines_for_tenants
            .entry(remote_tenant_id)
            .or_insert_with(|| {
                (
                    remote_timeline_id,
                    latest_timeline_lsn(remote_timeline_data),
                )
            });
        if latest_timeline_id != &remote_timeline_id {
            let mut remote_latest_lsn = latest_timeline_lsn(remote_timeline_data);
            if timeline_latest_lsn < &mut remote_latest_lsn {
                *latest_timeline_id = remote_timeline_id;
                *timeline_latest_lsn = remote_latest_lsn;
            }
        }
    }

    latest_timelines_for_tenants
        .into_iter()
        .map(|(&tenant_id, (&timeline_id, _))| (tenant_id, timeline_id))
        .collect()
}

// TODO kb is this a correct thing to do?
fn latest_timeline_lsn(timeline_data: &RemoteTimeline) -> Option<Lsn> {
    let latest_timeline_delta_lsn = timeline_data
        .delta_layers
        .iter()
        .map(|delta| delta.end_lsn)
        .max();
    let latest_timeline_image_lsn = timeline_data
        .image_layers
        .iter()
        .map(|image| image.lsn)
        .max();
    latest_timeline_delta_lsn.max(latest_timeline_image_lsn)
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

async fn download_timeline<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
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

    let sync_result = synchronize_layers(
        config,
        relish_storage,
        timeline_id,
        tenant_id,
        remote_timeline
            .image_layers
            .into_iter()
            .map(Layer::Image)
            .chain(remote_timeline.delta_layers.into_iter().map(Layer::Delta)),
        &metadata_path,
        SyncOperation::Download,
    )
    .await;

    match sync_result {
        SyncResult::Success { .. } => {}
        SyncResult::MetadataSyncError { .. } => {
            let download = RemoteTimeline {
                image_layers: BTreeSet::new(),
                delta_layers: BTreeSet::new(),
                ..remote_timeline
            };
            UPLOAD_QUEUE.lock().unwrap().push(if urgent {
                SyncTask::UrgentDownload(download)
            } else {
                SyncTask::Download(download)
            });
        }
        SyncResult::LayerSyncError {
            not_synced_image_layers,
            not_synced_delta_layers,
            ..
        } => {
            let download = RemoteTimeline {
                image_layers: not_synced_image_layers,
                delta_layers: not_synced_delta_layers,
                ..remote_timeline
            };
            UPLOAD_QUEUE.lock().unwrap().push(if urgent {
                SyncTask::UrgentDownload(download)
            } else {
                SyncTask::Download(download)
            });
        }
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

async fn upload_timeline<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    remote_timelines: &'a mut HashMap<(ZTenantId, ZTimelineId), RemoteTimeline>,
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

    let sync_result = synchronize_layers(
        config,
        relish_storage,
        timeline_id,
        tenant_id,
        new_upload
            .image_layers
            .into_iter()
            .map(Layer::Image)
            .chain(new_upload.delta_layers.into_iter().map(Layer::Delta)),
        &new_upload.metadata_path,
        SyncOperation::Upload,
    )
    .await;

    let entry_to_update = remote_timelines
        .entry((tenant_id, timeline_id))
        .or_insert_with(|| RemoteTimeline {
            image_layers: BTreeSet::new(),
            delta_layers: BTreeSet::new(),
            has_metadata: false,
            tenant_id,
            timeline_id,
        });
    match sync_result {
        SyncResult::Success {
            synced_image_layers,
            synced_delta_layers,
        } => {
            entry_to_update
                .image_layers
                .extend(synced_image_layers.into_iter());
            entry_to_update.has_metadata = true;
            entry_to_update
                .delta_layers
                .extend(synced_delta_layers.into_iter());
        }
        SyncResult::MetadataSyncError {
            synced_image_layers,
            synced_delta_layers,
        } => {
            entry_to_update
                .image_layers
                .extend(synced_image_layers.into_iter());
            entry_to_update
                .delta_layers
                .extend(synced_delta_layers.into_iter());
            UPLOAD_QUEUE
                .lock()
                .unwrap()
                .push(SyncTask::Upload(LocalTimeline {
                    image_layers: BTreeSet::new(),
                    delta_layers: BTreeSet::new(),
                    ..new_upload
                }));
        }
        SyncResult::LayerSyncError {
            synced_image_layers,
            synced_delta_layers,
            not_synced_image_layers,
            not_synced_delta_layers,
        } => {
            entry_to_update
                .image_layers
                .extend(synced_image_layers.into_iter());
            entry_to_update
                .delta_layers
                .extend(synced_delta_layers.into_iter());
            UPLOAD_QUEUE
                .lock()
                .unwrap()
                .push(SyncTask::Upload(LocalTimeline {
                    image_layers: not_synced_image_layers,
                    delta_layers: not_synced_delta_layers,
                    ..new_upload
                }));
        }
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

#[derive(Debug)]
enum Layer {
    Image(ImageFileName),
    Delta(DeltaFileName),
}

#[derive(Debug, Copy, Clone)]
enum SyncOperation {
    Download,
    Upload,
}

#[derive(Debug)]
enum SyncResult {
    Success {
        synced_image_layers: Vec<ImageFileName>,
        synced_delta_layers: Vec<DeltaFileName>,
    },
    MetadataSyncError {
        synced_image_layers: Vec<ImageFileName>,
        synced_delta_layers: Vec<DeltaFileName>,
    },
    LayerSyncError {
        synced_image_layers: Vec<ImageFileName>,
        synced_delta_layers: Vec<DeltaFileName>,
        not_synced_image_layers: BTreeSet<ImageFileName>,
        not_synced_delta_layers: BTreeSet<DeltaFileName>,
    },
}

async fn synchronize_layers<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    relish_storage: &'a S,
    timeline_id: ZTimelineId,
    tenant_id: ZTenantId,
    layers: impl Iterator<Item = Layer>,
    metadata_path: &'a Path,
    sync_operation: SyncOperation,
) -> SyncResult {
    let mut sync_operations = FuturesUnordered::new();
    // TODO kb put into config
    let concurrent_operations_limit = Arc::new(Semaphore::new(10));

    for layer in layers {
        let limit = Arc::clone(&concurrent_operations_limit);
        sync_operations.push(async move {
            let conf = PathOrConf::Conf(config);
            let layer_local_path = match &layer {
                Layer::Image(image_name) => {
                    ImageLayer::path_for(&conf, timeline_id, tenant_id, image_name)
                }
                Layer::Delta(delta_name) => {
                    DeltaLayer::path_for(&conf, timeline_id, tenant_id, delta_name)
                }
            };
            let permit = limit.acquire().await.expect("Semaphore is not closed yet");
            let sync_result = match sync_operation {
                SyncOperation::Download => {
                    download_relish(relish_storage, &config.workdir, &layer_local_path).await
                }
                SyncOperation::Upload => {
                    upload_relish(relish_storage, &config.workdir, &layer_local_path).await
                }
            };
            drop(permit);
            (layer, layer_local_path, sync_result)
        });
    }

    let mut synced_image_layers = Vec::with_capacity(sync_operations.len());
    let mut synced_delta_layers = Vec::with_capacity(sync_operations.len());
    let mut not_synced_image_layers = BTreeSet::new();
    let mut not_synced_delta_layers = BTreeSet::new();
    while let Some((layer, relish_local_path, relish_download_result)) =
        sync_operations.next().await
    {
        match (layer, relish_download_result) {
            (Layer::Image(image_layer), Ok(())) => synced_image_layers.push(image_layer),
            (Layer::Image(image_layer), Err(e)) => {
                log::error!(
                    "Failed to sync ({:?}) image layer {} with local path '{}', reason: {}",
                    sync_operation,
                    image_layer,
                    relish_local_path.display(),
                    e,
                );
                not_synced_image_layers.insert(image_layer);
            }
            (Layer::Delta(delta_layer), Ok(())) => {
                synced_delta_layers.push(delta_layer);
            }
            (Layer::Delta(delta_layer), Err(e)) => {
                log::error!(
                    "Failed to sync ({:?}) delta layer {} with local path '{}', reason: {}",
                    sync_operation,
                    delta_layer,
                    relish_local_path.display(),
                    e,
                );
                not_synced_delta_layers.insert(delta_layer);
            }
        }
    }
    concurrent_operations_limit.close();

    if not_synced_image_layers.is_empty() && not_synced_delta_layers.is_empty() {
        log::debug!(
            "Successfully uploaded all {} relishes",
            synced_image_layers.len() + synced_delta_layers.len(),
        );
        log::trace!("Uploaded image layers: {:?}", synced_image_layers);
        log::trace!("Uploaded delta layers: {:?}", synced_delta_layers);
        let metadata_sync_result = match sync_operation {
            SyncOperation::Download => {
                download_relish(relish_storage, &config.workdir, &metadata_path).await
            }
            SyncOperation::Upload => {
                upload_relish(relish_storage, &config.workdir, &metadata_path).await
            }
        };
        match metadata_sync_result {
            Ok(()) => {
                log::debug!("Metadata file synced successfully");
                SyncResult::Success {
                    synced_image_layers,
                    synced_delta_layers,
                }
            }
            Err(e) => {
                log::error!(
                    "Failed to sync ({:?} metadata file with local path '{}', reason: {}",
                    sync_operation,
                    metadata_path.display(),
                    e
                );
                SyncResult::MetadataSyncError {
                    synced_image_layers,
                    synced_delta_layers,
                }
            }
        }
    } else {
        SyncResult::LayerSyncError {
            synced_image_layers,
            synced_delta_layers,
            not_synced_delta_layers,
            not_synced_image_layers,
        }
    }
}
