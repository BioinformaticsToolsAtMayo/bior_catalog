-- MySQL dump 10.11
--
-- Host: localhost    Database: hg19
-- ------------------------------------------------------
-- Server version	5.0.67

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `sibTxGraph`
--

DROP TABLE IF EXISTS `sibTxGraph`;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
CREATE TABLE `sibTxGraph` (
  `bin` smallint(5) unsigned NOT NULL,
  `tName` varchar(255) NOT NULL default '',
  `tStart` int(11) NOT NULL default '0',
  `tEnd` int(11) NOT NULL default '0',
  `name` varchar(255) NOT NULL default '',
  `id` int(10) unsigned NOT NULL auto_increment,
  `strand` char(2) NOT NULL default '',
  `vertexCount` int(10) unsigned NOT NULL default '0',
  `vTypes` longblob NOT NULL,
  `vPositions` longblob NOT NULL,
  `edgeCount` int(10) unsigned NOT NULL default '0',
  `edgeStarts` longblob NOT NULL,
  `edgeEnds` longblob NOT NULL,
  `evidence` longblob NOT NULL,
  `edgeTypes` longblob NOT NULL,
  `mrnaRefCount` int(11) NOT NULL default '0',
  `mrnaRefs` longblob NOT NULL,
  `mrnaTissues` longblob NOT NULL,
  `mrnaLibs` longblob NOT NULL,
  PRIMARY KEY  (`id`),
  KEY `tName` (`tName`(16),`tStart`,`tEnd`)
) ENGINE=MyISAM AUTO_INCREMENT=46974 DEFAULT CHARSET=latin1;
SET character_set_client = @saved_cs_client;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2011-12-25 11:39:42
