CREATE TABLE IF NOT EXISTS traffic.raw_data
(
    track_id bigint,
    " type" text COLLATE pg_catalog."default",
    " traveled_d" double precision,
    " avg_speed" double precision,
    " lat" double precision,
    " lon" double precision,
    " speed" double precision,
    " lon_acc" double precision,
    " lat_acc" double precision,
    "time" double precision,
    other_data text COLLATE pg_catalog."default"
);