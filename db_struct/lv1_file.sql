/*
SQLyog Ultimate v12.09 (64 bit)
MySQL - 5.7.29-0ubuntu0.16.04.1-log : Database - microwavw_newdata
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`microwavw_newdata` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `microwavw_newdata`;

/*Table structure for table `t_lv1_file` */

DROP TABLE IF EXISTS `t_lv1_file`;

CREATE TABLE `t_lv1_file` (
  `id` varchar(256) NOT NULL,
  `wbfsj_id` int(11) DEFAULT NULL,
  `obs_time` datetime DEFAULT NULL,
  `file_path` varchar(256) DEFAULT NULL,
  `file_name` varchar(256) DEFAULT NULL,
  `isDelete` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
