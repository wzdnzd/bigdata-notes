drop table if exists `ct_call`

create table `ct_call`(
    `telephone` varchar(11) not null,
    `call_date` varchar(50) not null,
    `total_call` int(11) not null default 0,
    `total_duration` bigint not null default 0,
    primary key (`telephone`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;