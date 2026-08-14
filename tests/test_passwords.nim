import std/strutils
import std/unittest

import checksums/bcrypt
import checksums/md5

import ../nmqtt/utils/passwords

suite "nmqtt password hashing":
  test "creates and verifies a standard bcrypt password":
    let hashed = hashPassword("correct horse battery staple")

    check hashed.len == 60
    check hashed.startsWith("$2b$08$")
    check verifyPassword("correct horse battery staple", hashed)
    check not verifyPassword("wrong password", hashed)

  test "accepts the historical bcrypt plus external salt format":
    const
      password = "legacy-password"
      extraSalt = "legacy-extra-salt"
      fixedBcryptSalt = "$2a$08$LzUyyYdKBoEy9V4NTvxDH."

    let
      legacyInput = getMD5(extraSalt & getMD5(password))
      legacyHash = $bcrypt(legacyInput, parseSalt(fixedBcryptSalt))
      stored = legacyHash & extraSalt

    check stored.len > 60
    check verifyPassword(password, stored)
    check not verifyPassword("wrong password", stored)

  test "accepts the historical Windows MD5 plus external salt format":
    const
      password = "legacy-windows-password"
      extraSalt = "legacy-windows-extra-salt"

    let
      legacyHash = getMD5(extraSalt & getMD5(password))
      stored = legacyHash & extraSalt

    check verifyPassword(password, stored)
    check not verifyPassword("wrong password", stored)

  test "rejects malformed password records":
    check not verifyPassword("password", "")
    check not verifyPassword("password", "$2b$08$bad")
    check not verifyPassword("password", "not-a-password-hash")
