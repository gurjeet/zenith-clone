import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")


def test_xid_wraparound(pageserver, postgres, pg_bin, zenith_cli):

    # Create a branch for us
    zenith_cli.run(["branch", "test_xid_wraparound", "empty"])

    pg = postgres.create_start('test_xid_wraparound')
    print("postgres is running on 'test_xid_wraparound' branch")

    connstr = pg.connstr()

    conn = psycopg2.connect(connstr)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

 
    cur.execute('SELECT txid_current()')
    xid = cur.fetchone()[0]


    cur.execute('SELECT oldest_xid FROM pg_control_checkpoint()')
    oldest_xid = cur.fetchone()[0]

    print('before pgbench xid {} oldest_xid {}'.format(xid, oldest_xid))

    print('before pgbench xid {}'.format(xid))


    pg_bin.run_capture(['pgbench', '-i', connstr])
    pg_bin.run_capture(['pgbench'] + '-t 50000'.split() + [connstr])


    cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn = cur.fetchone()[0]
    print('LSN after pgbench: ' + lsn)
 
    cur.execute('SELECT txid_current()')
    xid = cur.fetchone()[0]

    cur.execute('vacuum freeze')

    cur.execute('SELECT oldest_xid FROM pg_control_checkpoint()')
    oldest_xid = cur.fetchone()[0]

    print('after pgbench xid {} oldest_xid {}'.format(xid, oldest_xid))

    zenith_cli.run(["branch", "test_xid_wraparound_new", "test_xid_wraparound@" + lsn])

    pg2 = postgres.create_start("test_xid_wraparound_new")

    conn2 = psycopg2.connect(pg2.connstr())
    cur2 = conn2.cursor()

    cur2.execute('SELECT txid_current()')
    xid = cur2.fetchone()[0]

    cur2.execute('SELECT oldest_xid FROM pg_control_checkpoint()')
    oldest_xid = cur2.fetchone()[0]

    print('new branch xid {} oldest_xid {}'.format(xid, oldest_xid))

    assert 1 == 0