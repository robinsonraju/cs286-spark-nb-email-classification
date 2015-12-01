mvn clean
mvn package
chmod 755 target/uber-nb-email-classifier-1.0.jar
scp -P 2222 target/uber-nb-email-classifier-1.0.jar user01@localhost:/user/user01/cs286_project
