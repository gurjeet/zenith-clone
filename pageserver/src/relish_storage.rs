//! Abstractions for the page server to store its relish layer data in the external storage.
//!
//! Main purpose of this module subtree is to provide a set of abstractions to manage the storage state
//! in a way, optimal for page server.
//!
//! The abstractions hide multiple custom external storage API implementations,
//! such as AWS S3, local filesystem, etc., located in the submodules.

mod local_fs;
mod rust_s3;
/// A queue-based storage with the background machinery behind it to synchronize
/// local page server layer files with external storage.
mod synced_storage;

use std::{
    path::{Path, PathBuf},
    thread,
};

use anyhow::{bail, Context};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use self::local_fs::LocalFs;
pub use self::synced_storage::StorageUploader;
use crate::{
    layered_repository::{
        filename::{DeltaFileName, ImageFileName},
        METADATA_FILE_NAME,
    },
    PageServerConf,
};

pub fn init_storage(
    config: &'static PageServerConf,
) -> anyhow::Result<Option<(StorageUploader, thread::JoinHandle<anyhow::Result<()>>)>> {
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
    let relish_storage =
        LocalFs::new(PathBuf::from("/home/someonetoignore/Downloads/tmp_dir")).unwrap();
    synced_storage::run_storage_sync_thread(config, relish_storage)
}

/// Storage (potentially remote) API to manage its state.
#[async_trait::async_trait]
pub trait RelishStorage: Send + Sync {
    type RelishStoragePath: std::fmt::Debug;

    fn storage_path(
        page_server_workdir: &Path,
        relish_local_path: &Path,
    ) -> anyhow::Result<Self::RelishStoragePath>;

    fn info(relish: &Self::RelishStoragePath) -> anyhow::Result<RelishInfo>;

    async fn list_relishes(&self) -> anyhow::Result<Vec<Self::RelishStoragePath>>;

    async fn download_relish(
        &self,
        from: &Self::RelishStoragePath,
        to: &Path,
    ) -> anyhow::Result<()>;

    async fn delete_relish(&self, path: &Self::RelishStoragePath) -> anyhow::Result<()>;

    async fn upload_relish(&self, from: &Path, to: &Self::RelishStoragePath) -> anyhow::Result<()>;
}

pub struct RelishInfo {
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    kind: RelishKind,
}

#[derive(Debug)]
pub enum RelishKind {
    Metadata,
    DeltaRelish(DeltaFileName),
    ImageRelish(ImageFileName),
}

fn strip_workspace_prefix<'a>(
    page_server_workdir: &'a Path,
    relish_local_path: &'a Path,
) -> anyhow::Result<&'a Path> {
    relish_local_path
        .strip_prefix(page_server_workdir)
        .with_context(|| {
            format!(
                "Unexpected: relish local path '{}' is not relevant to server workdir",
                relish_local_path.display(),
            )
        })
}

fn parse_relish_data(
    relish_data_segments: Option<(&str, &str, &str)>,
    relish_key: &str,
) -> anyhow::Result<RelishInfo> {
    let (tenant_id_str, timeline_id_str, relish_name_str) = match relish_data_segments {
        Some(data) => data,
        None => bail!("Cannot parse relish info out of the path '{}'", relish_key),
    };
    let tenant_id = tenant_id_str.parse::<ZTenantId>().with_context(|| {
        format!(
            "Failed to parse tenant id as part of relish path '{}'",
            relish_key
        )
    })?;
    let timeline_id = timeline_id_str.parse::<ZTimelineId>().with_context(|| {
        format!(
            "Failed to parse timeline id as part of relish path '{}'",
            relish_key
        )
    })?;

    let kind = if relish_name_str == METADATA_FILE_NAME {
        RelishKind::Metadata
    } else if let Some(delta_file_name) = DeltaFileName::from_str(relish_name_str) {
        RelishKind::DeltaRelish(delta_file_name)
    } else if let Some(image_file_name) = ImageFileName::from_str(relish_name_str) {
        RelishKind::ImageRelish(image_file_name)
    } else {
        bail!("Relish with key '{}' has an unknown file name", relish_key)
    };

    Ok(RelishInfo {
        tenant_id,
        timeline_id,
        kind,
    })
}
