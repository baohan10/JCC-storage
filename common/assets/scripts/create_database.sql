drop database if exists cloudream;

create database cloudream;

use cloudream;

create table Node (
  NodeID int not null auto_increment primary key comment '节点ID',
  Name varchar(128) not null comment '节点名称',
  LocalIP varchar(128) not null comment '节点的内网IP',
  ExternalIP varchar(128) not null comment '节点的外网IP',
  LocalGRPCPort int not null comment '节点的内网GRCP端口',
  ExternalGRPCPort int not null comment '节点的外网GRCP端口',
  LocationID int not null comment '节点的地域',
  State varchar(128) comment '节点的状态',
  LastReportTime timestamp comment '节点上次上报时间'
) comment = '节点表';

insert into
  Node (
    NodeID,
    Name,
    LocalIP,
    ExternalIP,
    LocationID,
    State
  )
values
  (0, "LocalNode", "localhost", "localhost", 0, 1);

create table Storage (
  StorageID int not null auto_increment primary key comment '存储服务ID',
  Name varchar(100) not null comment '存储服务名称',
  NodeID int not null comment '存储服务所在节点的ID',
  Directory varchar(4096) not null comment '存储服务所在节点的目录',
  State varchar(100) comment '状态'
) comment = "存储服务表";

insert into
  Storage (StorageID, Name, NodeID, Directory, State)
values
  (1, "HuaWei-Cloud", 1, "/", "Online");

create table NodeDelay (
  SourceNodeID int not null comment '发起检测的节点ID',
  DestinationNodeID int not null comment '被检测节点的ID',
  DelayInMs int not null comment '发起节点与被检测节点间延迟(毫秒)',
  primary key(SourceNodeID, DestinationNodeID)
) comment = '节点延迟表';

create table User (
  UserID int not null primary key comment '用户ID',
  Password varchar(100) not null comment '用户密码'
) comment = '用户密码表';

create table UserBucket (
  UserID int not null comment '用户ID',
  BucketID int not null comment '用户可访问的桶ID',
  primary key(UserID, BucketID)
) comment = '用户桶权限表';

insert into
  UserBucket (UserID, BucketID)
values
  (0, 1);

create table UserNode (
  UserID int not null comment '用户ID',
  NodeID int not null comment '用户可使用的节点ID',
  primary key(UserID, NodeID)
) comment = '用户节点权限表';

insert into
  UserNode (UserID, NodeID)
values
  (0, 1);

create table UserStorage (
  UserID int not null comment "用户ID",
  StorageID int not null comment "存储服务ID",
  primary key(UserID, StorageID)
);

insert into
  UserStorage (UserID, StorageID)
values
  (0, 1);

create table Bucket (
  BucketID int not null auto_increment primary key comment '桶ID',
  Name varchar(100) not null comment '桶名',
  CreatorID int not null comment '创建者ID'
) comment = '桶表';

insert into
  Bucket (BucketID, Name, CreatorID)
values
  (0, "bucket01", 0);

create table Package (
  PackageID int not null auto_increment primary key comment '包ID',
  Name varchar(100) not null comment '对象名',
  BucketID int not null comment '桶ID',
  State varchar(100) not null comment '状态',
  Redundancy JSON not null comment '冗余策略'
);

create table Object (
  ObjectID int not null auto_increment primary key comment '对象ID',
  PackageID int not null comment '包ID',
  Path varchar(500) not null comment '对象路径',
  Size bigint not null comment '对象大小(Byte)',
  UNIQUE KEY PackagePath (PackageID, Path)
) comment = '对象表';

create table ObjectRep (
  ObjectID int not null primary key comment '对象ID',
  FileHash varchar(100) not null comment '副本哈希值'
) comment = '对象副本表';

create table ObjectBlock (
  ObjectID int not null comment '对象ID',
  `Index` int not null comment '编码块在条带内的排序',
  FileHash varchar(100) not null comment '编码块哈希值',
  primary key(ObjectID, `Index`)
) comment = '对象编码块表';

create table Cache (
  FileHash varchar(100) not null comment '编码块块ID',
  NodeID int not null comment '节点ID',
  State varchar(100) not null comment '状态',
  CacheTime timestamp not null comment '缓存时间',
  Priority int not null comment '编码块优先级',
  primary key(FileHash, NodeID)
) comment = '缓存表';

create table StoragePackage (
  PackageID int not null comment '包ID',
  StorageID int not null comment '存储服务ID',
  UserID int not null comment '调度了此文件的用户ID',
  State varchar(100) not null comment '包状态',
  primary key(PackageID, StorageID, UserID)
);

create table Location (
  LocationID int not null auto_increment primary key comment 'ID',
  Name varchar(128) not null comment '名称'
) comment = '地域表';

insert into
  Location (LocationID, Name)
values
  (1, "Local");

create table Ec (
  EcID int not null comment '纠删码ID',
  Name varchar(128) not null comment '纠删码名称',
  EcK int not null comment 'ecK',
  EcN int not null comment 'ecN'
) comment = '纠删码表';

insert into
  Ec (EcID, Name, EcK, EcN)
values
  (1, "rs_9_6", 6, 9);

insert into
  Ec (EcID, Name, EcK, EcN)
values
  (2, "rs_5_3", 3, 5);