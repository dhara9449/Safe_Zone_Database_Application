CREATE DATABASE publicsafety;

CREATE TABLE `publicsafety`.`zone` (
  `zoneId` INT NOT NULL,
  `zoneName` VARCHAR(45) NOT NULL,
  `squadNumber` INT NOT NULL,
  `verticesCount` INT NOT NULL,
  `coordinates` POLYGON NOT NULL SRID 0, SPATIAL INDEX(`coordinates`),
  PRIMARY KEY (`zoneId`));
  
  CREATE TABLE `publicsafety`.`officer` (
  `badgeNum` INT NOT NULL,
  `officerName` VARCHAR(45) NOT NULL,
  `squadNum` VARCHAR(45) NOT NULL,
  `currentLocation` POINT NOT NULL SRID 0, SPATIAL INDEX(`currentLocation`),
  PRIMARY KEY (`badgeNum`));

  CREATE TABLE `publicsafety`.`route` (
  `routeNum` INT NOT NULL,
  `vehiclesCount` INT NOT NULL,
  `coordinates` LINESTRING NOT NULL SRID 0, SPATIAL INDEX(`coordinates`),
  PRIMARY KEY (`routeNum`));

  
  CREATE TABLE `publicsafety`.`incident` (
  `incidentId` INT NOT NULL,
  `incidentType` VARCHAR(45) NOT NULL,
  `incidentLocation` POINT NOT NULL SRID 0, SPATIAL INDEX(`incidentLocation`),
  PRIMARY KEY (`incidentId`));

