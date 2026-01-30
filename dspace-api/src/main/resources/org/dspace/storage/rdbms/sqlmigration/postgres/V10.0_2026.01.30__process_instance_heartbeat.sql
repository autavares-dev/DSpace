--
-- The contents of this file are subject to the license and copyright
-- detailed in the LICENSE and NOTICE files at the root of the source
-- tree and available online at
--
-- http://www.dspace.org/license/
--

-----------------------------------------------------------------------------------
-- Alter process table to enable the processes heartbeat feature and to properly
-- fail stale process in multi-instance deployments.
-----------------------------------------------------------------------------------
ALTER TABLE process ADD COLUMN instance_id UUID NULL;
ALTER TABLE process ADD COLUMN last_heartbeat TIMESTAMP NULL;
