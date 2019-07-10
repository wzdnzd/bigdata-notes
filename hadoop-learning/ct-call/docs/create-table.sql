drop table if exists `ct_call`;

create table `ct_call`
(
    `id`             int(11) auto_increment,
    `telephone`      varchar(11) not null,
    `call_date`      varchar(50) not null,
    `total_call`     int(11)     not null default 0,
    `total_duration` bigint      not null default 0,
    primary key (`id`),
    index (`telephone`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;