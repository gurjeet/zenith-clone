import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")

def test_xid_wraparound(pageserver, postgres, pg_bin, zenith_cli):

    # Create a branch for us
    zenith_cli.run(["branch", "test_xid_wraparound", "empty"])

    #set agressive autovacuum
    config = ['autovacuum_max_workers=10',
              'autovacuum_naptime=5',
              'autovacuum_freeze_max_age=100000',
              'vacuum_freeze_min_age=0',
              'vacuum_freeze_table_age=0',
              'log_autovacuum_min_duration=0']
    pg = postgres.create_start('test_xid_wraparound', config_lines=config)
    print("postgres is running on 'test_xid_wraparound' branch")

    connstr = pg.connstr()

    conn = psycopg2.connect(connstr)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # just a bit of debug output
    cur.execute('SELECT txid_current()')
    xid = cur.fetchone()[0]
    cur.execute('SELECT oldest_xid FROM pg_control_checkpoint()')
    oldest_xid = cur.fetchone()[0]
    print('before pgbench xid {} oldest_xid {}'.format(xid, oldest_xid))

    # perform enough transactions to trigger xid wraparound
    pg_bin.run_capture(['pgbench', '-i', connstr])
    pg_bin.run_capture(['pgbench'] + '-t 50000'.split() + [connstr])

    cur.execute('vacuum freeze')
    cur.execute('checkpoint')
 
    cur.execute('SELECT txid_current()')
    xid1 = cur.fetchone()[0]
    cur.execute('SELECT oldest_xid, oldest_xid_dbid FROM pg_control_checkpoint()')
    res = cur.fetchone()
    oldest_xid1 = res[0]
    oldest_xid_dbid1 = res[1]
    print('after pgbench xid {} oldest_xid {} oldest_xid_dbid2 {}'.format(xid1, oldest_xid1, oldest_xid_dbid1))

    # ensure that oldest_xid advanced
    assert int(oldest_xid) < int(oldest_xid1)

    cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn = cur.fetchone()[0]
    print('LSN after pgbench: ' + lsn)

    # create new branch that will start from wraparounded state
    zenith_cli.run(["branch", "test_xid_wraparound_new", "test_xid_wraparound@" + lsn])
    pg2 = postgres.create_start("test_xid_wraparound_new")

    conn2 = psycopg2.connect(pg2.connstr())
    cur2 = conn2.cursor()

    cur2.execute('SELECT txid_current()')
    xid2 = cur2.fetchone()[0]
    cur2.execute('SELECT oldest_xid, oldest_xid_dbid FROM pg_control_checkpoint()')
    res2 = cur2.fetchone()
    oldest_xid2 = res2[0]
    oldest_xid_dbid2 = res2[1]
    print('new branch xid {} oldest_xid {} oldest_xid_dbid2 {}'.format(xid2, oldest_xid2, oldest_xid_dbid2))

    # ensure that we restored fields correctly
    assert int(xid1) <= int(xid2)
    assert int(oldest_xid1) == int(oldest_xid2)
    assert int(oldest_xid_dbid1) == int(oldest_xid_dbid2)
