//!
//! WAL receiver connects to the WAL safekeeper service, streams WAL,
//! decodes records and saves them in the repository for the correct
//! timeline.
//!
//! We keep one WAL receiver active per timeline.

use crate::relish::RelishTag;
use crate::repository::Timeline;
use crate::restore_local_repo;
use crate::tenant_mgr;
use crate::waldecoder::{decode_wal_record, WalStreamDecoder};
use crate::PageServerConf;
use anyhow::{Context, Error, Result};
use bytes::Bytes;
use lazy_static::lazy_static;
use log::*;
use postgres::fallible_iterator::FallibleIterator;
use postgres::replication::ReplicationIter;
use postgres::{Client, NoTls, SimpleQueryMessage, SimpleQueryRow};
use postgres_ffi::xlog_utils::{IsXLogFileName, XLogFromFileName};
use postgres_ffi::{pg_constants, CheckPoint};
use postgres_protocol::PG_EPOCH;
use postgres_types::PgLsn;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Mutex;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use zenith_utils::lsn::Lsn;
use zenith_utils::pq_proto::XLogDataBody;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

/// Trait to generalize receiving the WAL over different connection types
///
/// The primary usage of this trait is in [`walreceiver_loop`], which performs the bulk of
/// the required logic relating to receiving the WAL.
trait WalReceiverConnection {
    /// Blocks until receiving a replication message over the connection
    ///
    /// If the connection is closed, return `Err(None)`
    fn receive_message(&mut self) -> Result<Option<ReplicationMessage>>;

    /// Sends a `StandbyStatusUpdate` message over the connection
    fn send_standby_status_update(
        &mut self,
        write_lsn: Lsn,
        flush_lsn: Lsn,
        apply_lsn: Lsn,
        ts: SystemTime,
    ) -> Result<()>;
}

enum ReplicationMessage {
    XLogData(XLogDataBody<Bytes>),
    PrimaryKeepAlive(PrimaryKeepAliveBody),
}

#[derive(Serialize, Deserialize)]
struct PrimaryKeepAliveBody {
    wal_end: u64,
    timestamp: i64,
    reply: u8,
}

impl<'a, C: WalReceiverConnection> WalReceiverConnection for &'a mut C {
    fn receive_message(&mut self) -> Result<Option<ReplicationMessage>> {
        (*self).receive_message()
    }

    fn send_standby_status_update(
        &mut self,
        write_lsn: Lsn,
        flush_lsn: Lsn,
        apply_lsn: Lsn,
        ts: SystemTime,
    ) -> Result<()> {
        (*self).send_standby_status_update(write_lsn, flush_lsn, apply_lsn, ts)
    }
}

/// Network connection for a standard postgres replication protocol client
///
/// This connection receives the WAL from postgres itself by connecting after a
/// 'callmemaybe' request.
pub struct StandardWalReceiver<'client> {
    stream: ReplicationIter<'client>,
}

impl<'client> WalReceiverConnection for StandardWalReceiver<'client> {
    fn receive_message(&mut self) -> Result<Option<ReplicationMessage>> {
        use postgres_protocol::message::backend::ReplicationMessage::{PrimaryKeepAlive, XLogData};

        let msg = match self.stream.next()? {
            Some(m) => m,
            None => return Ok(None),
        };

        match msg {
            XLogData(xld) => {
                let xlog_data = XLogDataBody {
                    wal_start: xld.wal_start(),
                    wal_end: xld.wal_end(),
                    timestamp: match xld.timestamp().duration_since(*PG_EPOCH) {
                        Ok(d) => d.as_micros() as i64,
                        Err(e) => -(e.duration().as_micros() as i64),
                    },
                    data: Bytes::clone(xld.data()),
                };

                Ok(Some(ReplicationMessage::XLogData(xlog_data)))
            }
            PrimaryKeepAlive(pka) => {
                let keepalive = PrimaryKeepAliveBody {
                    wal_end: pka.wal_end(),
                    timestamp: match pka.timestamp().duration_since(*PG_EPOCH) {
                        Ok(d) => d.as_micros() as i64,
                        Err(e) => -(e.duration().as_micros() as i64),
                    },
                    reply: pka.reply(),
                };

                Ok(Some(ReplicationMessage::PrimaryKeepAlive(keepalive)))
            }
            // We have to have the wildcard at the bottom here because
            // `ReplicationMessage` is marked as non-exhaustive.
            _ => panic!("unrecognized replication message"),
        }
    }

    fn send_standby_status_update(
        &mut self,
        write_lsn: Lsn,
        flush_lsn: Lsn,
        apply_lsn: Lsn,
        ts: SystemTime,
    ) -> Result<()> {
        const NO_REPLY: u8 = 0;

        self.stream.standby_status_update(
            write_lsn.into(),
            flush_lsn.into(),
            apply_lsn.into(),
            ts,
            NO_REPLY,
        )?;

        Ok(())
    }
}

lazy_static! {
    /// The active connections with `StandardWalReceiver` and their connection strings to the
    /// replication server
    static ref WAL_RECEIVERS: Mutex<HashMap<ZTimelineId, WalReceiverEntry>> =
        Mutex::new(HashMap::new());
}

/// Entry in WAL_RECEIVERS, storing the connection string for each standard connection to a
/// replication server
struct WalReceiverEntry {
    wal_producer_connstr: String,
}

impl<'c> StandardWalReceiver<'c> {
    /// Main entrypoint for handling 'callemaybe'
    ///
    /// Launch a new standard WAL receiver, or tell one that's running about a change in the
    /// connection string
    pub fn launch(
        conf: &'static PageServerConf,
        wal_producer_connstr: String,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
    ) {
        let mut receivers = WAL_RECEIVERS.lock().unwrap();

        match receivers.get_mut(&timelineid) {
            Some(receiver) => {
                receiver.wal_producer_connstr = wal_producer_connstr;
            }
            None => {
                let receiver = WalReceiverEntry {
                    wal_producer_connstr,
                };
                receivers.insert(timelineid, receiver);

                // Also launch a new thread to handle this connection
                //
                // NOTE: This thread name is checked in the assertion in wait_lsn. If you change
                // this, make sure you update the assertion too.
                let _walreceiver_thread = thread::Builder::new()
                    .name("WAL receiver thread".into())
                    .spawn(move || {
                        Self::thread_main(conf, timelineid, tenantid);
                    })
                    .unwrap();
            }
        };
    }

    /// Look up current WAL producer connection string in the hash table
    fn get_wal_producer_connstr(timelineid: ZTimelineId) -> String {
        let receivers = WAL_RECEIVERS.lock().unwrap();

        receivers
            .get(&timelineid)
            .unwrap()
            .wal_producer_connstr
            .clone()
    }

    ///
    /// This is the entry point for the WAL receiver thread.
    ///
    fn thread_main(conf: &'static PageServerConf, timelineid: ZTimelineId, tenantid: ZTenantId) {
        info!(
            "WAL receiver thread started for timeline : '{}'",
            timelineid
        );

        //
        // Make a connection to the WAL safekeeper, or directly to the primary PostgreSQL server,
        // and start streaming WAL from it. If the connection is lost, keep retrying.
        //
        loop {
            // Look up the current WAL producer address
            let wal_producer_connstr = Self::get_wal_producer_connstr(timelineid);

            let res = Self::walreceiver_main(conf, timelineid, &wal_producer_connstr, tenantid);

            if let Err(e) = res {
                info!(
                    "WAL streaming connection failed ({}), retrying in 1 second",
                    e
                );
                sleep(Duration::from_secs(1));
            }
        }
    }

    fn walreceiver_main(
        conf: &PageServerConf,
        timelineid: ZTimelineId,
        wal_producer_connstr: &str,
        tenantid: ZTenantId,
    ) -> Result<(), Error> {
        // Connect to the database in replication mode.
        info!("connecting to {:?}", wal_producer_connstr);
        let connect_cfg = format!(
            "{} application_name=pageserver replication=true",
            wal_producer_connstr
        );

        let mut rclient = Client::connect(&connect_cfg, NoTls)?;
        info!("connected!");

        // Immediately increment the gauge, then create a job to decrement it on thread exit.
        // One of the pros of `defer!` is that this will *most probably*
        // get called, even in presence of panics.
        let gauge = crate::LIVE_CONNECTIONS_COUNT.with_label_values(&["wal_receiver"]);
        gauge.inc();
        scopeguard::defer! {
            gauge.dec();
        }

        let identify = identify_system(&mut rclient)?;
        info!("{:?}", identify);
        let end_of_wal = Lsn::from(u64::from(identify.xlogpos));

        let timeline = tenant_mgr::get_timeline_for_tenant(tenantid, timelineid)?;

        let (last_rec_lsn, startpoint) = get_last_rec_lsn_and_startpoint(&*timeline);

        let query = format!("START_REPLICATION PHYSICAL {}", startpoint);

        let copy_stream = rclient.copy_both_simple(&query)?;

        let conn = StandardWalReceiver {
            stream: ReplicationIter::new(copy_stream),
        };

        walreceiver_loop(
            conn,
            conf,
            tenantid,
            timelineid,
            &*timeline,
            last_rec_lsn,
            startpoint,
            end_of_wal,
        )?;

        Ok(())
    }
}

/// Helper function, to be called before `walreceiver_loop`: returns (last_rec_lsn, startpoint)
fn get_last_rec_lsn_and_startpoint(timeline: &dyn Timeline) -> (Lsn, Lsn) {
    // If we had previously received WAL up to some point in the middle of a WAL record, we
    // better start from the end of last full WAL record, not in the middle of one.
    let last_rec_lsn = timeline.get_last_record_lsn();
    let startpoint = last_rec_lsn;

    (last_rec_lsn, startpoint)
}

/// Central logic for receiving the WAL, generic over the connection we get it from
fn walreceiver_loop(
    mut conn: impl WalReceiverConnection,
    conf: &PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    timeline: &dyn Timeline,
    mut last_rec_lsn: Lsn,
    mut startpoint: Lsn,
    end_of_wal: Lsn, // Server's xlogpos
) -> Result<()> {
    let mut caught_up = false;

    if startpoint == Lsn(0) {
        error!("No previous WAL position for timeline {}", timelineid);
    }

    // There might be some padding after the last full record, skip it.
    startpoint += startpoint.calc_padding(8u32);

    info!(
        "last_record_lsn {} starting replication from {} for timeline {}, server is at {}...",
        last_rec_lsn, startpoint, timelineid, end_of_wal
    );

    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let checkpoint_bytes = timeline.get_page_at_lsn(RelishTag::Checkpoint, 0, startpoint)?;
    let mut checkpoint = CheckPoint::decode(&checkpoint_bytes)?;
    trace!("CheckPoint.nextXid = {}", checkpoint.nextXid.value);

    while let Some(replication_message) = conn
        .receive_message()
        .context("failed to read replication message")?
    {
        let status_update = match replication_message {
            ReplicationMessage::XLogData(xlog_data) => {
                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                let data = &xlog_data.data;
                let startlsn = Lsn::from(xlog_data.wal_start);
                let endlsn = startlsn + data.len() as u64;
                let prev_last_rec_lsn = last_rec_lsn;

                trace!("received XLogData between {} and {}", startlsn, endlsn);

                waldecoder.feed_bytes(data);

                while let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                    // Save old checkpoint value to compare with it after decoding WAL record
                    let old_checkpoint_bytes = checkpoint.encode();
                    let decoded = decode_wal_record(recdata.clone());

                    // It is important to deal with the aligned records as lsn in getPage@LSN is
                    // aligned and can be several bytes bigger. Without this alignment we are
                    // at risk of hittind a deadlock.
                    assert!(lsn.is_aligned());

                    restore_local_repo::save_decoded_record(
                        &mut checkpoint,
                        &*timeline,
                        &decoded,
                        recdata,
                        lsn,
                    )?;

                    let new_checkpoint_bytes = checkpoint.encode();
                    // Check if checkpoint data was updated by save_decoded_record
                    if new_checkpoint_bytes != old_checkpoint_bytes {
                        timeline.put_page_image(
                            RelishTag::Checkpoint,
                            0,
                            lsn,
                            new_checkpoint_bytes,
                        )?;
                    }

                    // Now that this record has been fully handled, including updating the
                    // checkpoint data, let the repository know that it is up-to-date to this LSN
                    timeline.advance_last_record_lsn(lsn);
                    last_rec_lsn = lsn;
                }

                // Somewhat arbitrarily, if we have at least 10 complete wal segments (16 MB each),
                // "checkpoint" the repository to flush all the changes from WAL we've processed
                // so far to disk. After this, we don't need the original WAL anymore, and it
                // can be removed. This is probably too aggressive for production, but it's useful
                // to expose bugs now.
                //
                // TODO: We don't actually dare to remove the WAL. It's useful for debugging,
                // and we might it for logical decoding other things in the future. Although
                // we should also be able to fetch it back from the WAL safekeepers or S3 if
                // needed.
                if prev_last_rec_lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE)
                    != last_rec_lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE)
                {
                    info!("switched segment {} to {}", prev_last_rec_lsn, last_rec_lsn);
                    let (oldest_segno, newest_segno) = find_wal_file_range(
                        conf,
                        &timelineid,
                        pg_constants::WAL_SEGMENT_SIZE,
                        last_rec_lsn,
                        &tenantid,
                    )?;

                    if newest_segno - oldest_segno >= 10 {
                        // TODO: This is where we could remove WAL older than last_rec_lsn.
                        //remove_wal_files(timelineid, pg_constants::WAL_SEGMENT_SIZE, last_rec_lsn)?;
                    }
                }

                if !caught_up && endlsn >= end_of_wal {
                    info!("caught up at LSN {}", endlsn);
                    caught_up = true;
                }

                Some(endlsn)
            }

            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let reply_requested = keepalive.reply != 0;

                trace!(
                    "received PrimaryKeepAlive(wal_end: {}, timestamp: {:?} reply: {})",
                    keepalive.wal_end,
                    keepalive.timestamp,
                    reply_requested,
                );

                if reply_requested {
                    Some(timeline.get_last_record_lsn())
                } else {
                    None
                }
            }
        };

        if let Some(last_lsn) = status_update {
            // TODO: More thought should go into what values are sent here.
            let write_lsn = last_lsn;
            let flush_lsn = last_lsn;
            let apply_lsn = Lsn::from(0);
            let ts = SystemTime::now();

            conn.send_standby_status_update(write_lsn, flush_lsn, apply_lsn, ts)?;
        }
    }

    Ok(())
}

fn find_wal_file_range(
    conf: &PageServerConf,
    timeline: &ZTimelineId,
    wal_seg_size: usize,
    written_upto: Lsn,
    tenant: &ZTenantId,
) -> Result<(u64, u64)> {
    let written_upto_segno = written_upto.segment_number(wal_seg_size);

    let mut oldest_segno = written_upto_segno;
    let mut newest_segno = written_upto_segno;
    // Scan the wal directory, and count how many WAL filed we could remove
    let wal_dir = conf.wal_dir_path(timeline, tenant);
    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            continue;
        }

        let filename = path.file_name().unwrap().to_str().unwrap();

        if IsXLogFileName(filename) {
            let (segno, _tli) = XLogFromFileName(filename, wal_seg_size);

            if segno > written_upto_segno {
                // that's strange.
                warn!("there is a WAL file from future at {}", path.display());
                continue;
            }

            oldest_segno = min(oldest_segno, segno);
            newest_segno = max(newest_segno, segno);
        }
    }
    // FIXME: would be good to assert that there are no gaps in the WAL files

    Ok((oldest_segno, newest_segno))
}

/// Data returned from the postgres `IDENTIFY_SYSTEM` command
///
/// See the [postgres docs] for more details.
///
/// [postgres docs]: https://www.postgresql.org/docs/current/protocol-replication.html
#[derive(Debug)]
// As of nightly 2021-09-11, fields that are only read by the type's `Debug` impl still count as
// unused. Relevant issue: https://github.com/rust-lang/rust/issues/88900
#[allow(dead_code)]
pub struct IdentifySystem {
    systemid: u64,
    timeline: u32,
    xlogpos: PgLsn,
    dbname: Option<String>,
}

/// There was a problem parsing the response to
/// a postgres IDENTIFY_SYSTEM command.
#[derive(Debug, thiserror::Error)]
#[error("IDENTIFY_SYSTEM parse error")]
pub struct IdentifyError;

/// Run the postgres `IDENTIFY_SYSTEM` command
pub fn identify_system(client: &mut Client) -> Result<IdentifySystem, Error> {
    let query_str = "IDENTIFY_SYSTEM";
    let response = client.simple_query(query_str)?;

    // get(N) from row, then parse it as some destination type.
    fn get_parse<T>(row: &SimpleQueryRow, idx: usize) -> Result<T, IdentifyError>
    where
        T: FromStr,
    {
        let val = row.get(idx).ok_or(IdentifyError)?;
        val.parse::<T>().or(Err(IdentifyError))
    }

    // extract the row contents into an IdentifySystem struct.
    // written as a closure so I can use ? for Option here.
    if let Some(SimpleQueryMessage::Row(first_row)) = response.get(0) {
        Ok(IdentifySystem {
            systemid: get_parse(first_row, 0)?,
            timeline: get_parse(first_row, 1)?,
            xlogpos: get_parse(first_row, 2)?,
            dbname: get_parse(first_row, 3).ok(),
        })
    } else {
        Err(IdentifyError.into())
    }
}
