@echo on

@rem set dockerimage and Build
@set dockerimage=ppmp2influxconnector
@set Build=""


@rem schreibe git-versions-build into  dockerversion.txt
@git describe --tags --abbrev=0  > dockerversion.txt

@rem lese aus Datei dockerversion.txt die Version und schreibe diese into Build
@set /p Build=<dockerversion.txt

@IF %Build% EQU "" set Build="latest"


@echo Build docker image %dockerimage%:%Build%

@rem build Docker image aus parametern
@docker build  . -t %dockerimage%:%Build%

@echo create docker_images directory
@MKDIR  ..\docker_images

@docker save -o ../docker_images/%dockerimage%_%Build%.zip %dockerimage%:%Build%

@echo docker save image  ../docker_images/%dockerimage%_%Build%.zip