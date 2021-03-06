create table if not exists jenkins_versions (
    id int generated by default as identity primary key,
    version varchar(32)
);

create unique index jenkins_version_version on jenkins_versions(version);

create table if not exists jvm_versions (
    id int generated by default as identity primary key,
    name text NOT NULL
);

create unique index jvm_version_name on jvm_versions(name);

CREATE TABLE IF NOT EXISTS instance_reports (
    id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    instance_id varchar(64) NOT NULL,
    year smallint NOT NULL,
    month smallint NOT NULL,
    count_for_month int default 0,
    report_time timestamptz NOT NULL,
    version int references jenkins_versions,
    jvm_version_id int references jvm_versions,
    executors int default 0,
    plugins int[],
    jobs jsonb,
    nodes jsonb
);

create index instance_reports_year_month on instance_reports using btree(year, month);
create unique index instance_reports_instance_id_year_month on instance_reports using btree(instance_id, year, month);

CREATE TABLE IF NOT EXISTS plugins (
    id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    version text NOT NULL
);

create unique index plugins_name_and_version on plugins using btree(name, version);
create index plugins_name on plugins(name);

CREATE TABLE IF NOT EXISTS job_types (
    id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name text NOT NULL
);

create unique index job_type_name on job_types(name);

CREATE TABLE IF NOT EXISTS os_types (
     id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
     name text NOT NULL
);

create unique index os_type_name on os_types(name);

create table if not exists report_files (
    filename text primary key
)
