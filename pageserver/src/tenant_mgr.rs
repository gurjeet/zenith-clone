//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use std::{
    collections::HashMap,
    fs,
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, bail, Context, Result};
use lazy_static::lazy_static;
use log::info;

use zenith_utils::zid::{ZTenantId, ZTimelineId};

use crate::branches;
use crate::layered_repository::LayeredRepository;
use crate::relish_storage::StorageUploader;
use crate::repository::{Repository, Timeline};
use crate::walredo::PostgresRedoManager;
use crate::PageServerConf;

lazy_static! {
    static ref REPOSITORY: Mutex<HashMap<ZTenantId, Arc<dyn Repository>>> =
        Mutex::new(HashMap::new());
}

pub fn init(conf: &'static PageServerConf, storage_uploader: Option<StorageUploader>) {
    let mut m = REPOSITORY.lock().unwrap();
    let storage_uploader = storage_uploader.map(Arc::new);

    for dir_entry in fs::read_dir(conf.tenants_path()).unwrap() {
        let tenantid =
            ZTenantId::from_str(dir_entry.unwrap().file_name().to_str().unwrap()).unwrap();

        // Set up a WAL redo manager, for applying WAL records.
        let walredo_mgr = PostgresRedoManager::new(conf, tenantid);

        // Set up an object repository, for actual data storage.
        let repo = Arc::new(LayeredRepository::new(
            conf,
            Arc::new(walredo_mgr),
            tenantid,
            storage_uploader.as_ref().map(Arc::clone),
        ));
        LayeredRepository::launch_checkpointer_thread(conf, repo.clone());
        LayeredRepository::launch_gc_thread(conf, repo.clone());

        info!("initialized storage for tenant: {}", &tenantid);
        m.insert(tenantid, repo);
    }
}

pub fn create_repository_for_tenant(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
) -> Result<()> {
    let mut m = REPOSITORY.lock().unwrap();

    // First check that the tenant doesn't exist already
    if m.get(&tenantid).is_some() {
        bail!("tenant {} already exists", tenantid);
    }
    let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenantid));
    let repo = branches::create_repo(conf, tenantid, wal_redo_manager)?;

    m.insert(tenantid, repo);

    Ok(())
}

pub fn insert_repository_for_tenant(tenantid: ZTenantId, repo: Arc<dyn Repository>) {
    let o = &mut REPOSITORY.lock().unwrap();
    o.insert(tenantid, repo);
}

pub fn get_repository_for_tenant(tenantid: ZTenantId) -> Result<Arc<dyn Repository>> {
    let o = &REPOSITORY.lock().unwrap();
    o.get(&tenantid)
        .map(|repo| Arc::clone(repo))
        .ok_or_else(|| anyhow!("repository not found for tenant name {}", tenantid))
}

pub fn get_timeline_for_tenant(
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
) -> Result<Arc<dyn Timeline>> {
    get_repository_for_tenant(tenantid)?
        .get_timeline(timelineid)
        .with_context(|| format!("cannot fetch timeline {}", timelineid))
}
