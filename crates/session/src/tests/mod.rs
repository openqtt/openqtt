//! Tests of the machine, each tagged with the R1 statements it proves: by its name,
//! `mqtt_<x>_<y>_<z>_<n>_...`, or on a `covers:` line (report R1, Tests carry statement ids).

mod harness;

mod connect;
mod deliver;
mod publish;
