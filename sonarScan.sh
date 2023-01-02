export SONAR_SCANNER_VERSION=4.7.0.2747
export SONAR_SCANNER_HOME=$HOME/.sonar/sonar-scanner-$SONAR_SCANNER_VERSION-macosx
curl --create-dirs -sSLo $HOME/.sonar/sonar-scanner.zip https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-$SONAR_SCANNER_VERSION-macosx.zip
unzip -o $HOME/.sonar/sonar-scanner.zip -d $HOME/.sonar/
export PATH=$SONAR_SCANNER_HOME/bin:$PATH
export SONAR_SCANNER_OPTS="-server"

SONAR_TOKEN=sqp_f1d6258593bb0c12081332e91d23760770020b5b sonar-scanner \
  -Dsonar.projectKey=DB \
  -Dsonar.sources=. \
  -Dsonar.host.url=http://lion:9000
