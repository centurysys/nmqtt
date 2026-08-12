# Package
version       = "1.1.1"
author        = "zevv & ThomasTJdev, Takeyoshi Kikuchi"
description   = "Native MQTT library and binaries for publishing, subscribing and broker"
license       = "MIT"
#bin           = @["nmqtt/nmqtt", "nmqtt/nmqtt_password", "nmqtt/nmqtt_pub", "nmqtt/nmqtt_sub"]
#binDir        = "bin"
installFiles  = @["nmqtt_ng.nim"]
installDirs   = @["nmqttngpkgs"]
#skipDirs      = @["tests", "nmqtt"]

# Dependencies
requires "nim >= 2.2.10"
requires "cligen >= 0.9.45"
