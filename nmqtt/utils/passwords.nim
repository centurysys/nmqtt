import std/strutils

import checksums/bcrypt
import checksums/md5

const
  PasswordBcryptCost*: CostFactor = 8
  BcryptHashLength = 60
  Md5HexLength = 32

# ------------------------------------------------------------------------------
# New password entries use the standard bcrypt representation directly.
# generateSalt() obtains randomness from std/sysrand inside checksums/bcrypt.
# ------------------------------------------------------------------------------
proc hashPassword*(password: string): string =
  result = $bcrypt(password, generateSalt(PasswordBcryptCost))

# ------------------------------------------------------------------------------
# Older Unix nmqtt_password versions stored:
#
#   <60-byte bcrypt hash><extra random salt>
#
# where bcrypt was calculated over:
#
#   MD5(extraSalt & MD5(password))
#
# Keep accepting that representation so existing password files do not need to
# be regenerated when upgrading nmqtt_ng.
# ------------------------------------------------------------------------------
proc verifyLegacyBcryptPassword(password, stored: string): bool =
  if stored.len <= BcryptHashLength:
    return false

  let
    storedHash = stored[0 ..< BcryptHashLength]
    extraSalt = stored[BcryptHashLength .. ^1]

  if not (
      storedHash.startsWith("$2a$") or
      storedHash.startsWith("$2b$") or
      storedHash.startsWith("$2y$")
  ):
    return false

  try:
    let legacyInput = getMD5(extraSalt & getMD5(password))
    result = $bcrypt(legacyInput, parseSalt(storedHash)) == storedHash
  except ValueError:
    result = false

# ------------------------------------------------------------------------------
# Older Windows nmqtt_password versions used:
#
#   MD5(extraSalt & MD5(password))<extra random salt>
#
# This was never a strong password format, but accepting it here preserves
# compatibility with password files created by those versions.
# ------------------------------------------------------------------------------
proc verifyLegacyMd5Password(password, stored: string): bool =
  if stored.len <= Md5HexLength:
    return false

  let
    storedHash = stored[0 ..< Md5HexLength]
    extraSalt = stored[Md5HexLength .. ^1]

  result = getMD5(extraSalt & getMD5(password)) == storedHash

# ------------------------------------------------------------------------------
# Accept the current standard bcrypt format first, then the two historical
# nmqtt formats. Invalid or malformed password records simply fail validation.
# ------------------------------------------------------------------------------
proc verifyPassword*(password, stored: string): bool =
  if stored.len == BcryptHashLength and (
      stored.startsWith("$2a$") or
      stored.startsWith("$2b$") or
      stored.startsWith("$2y$")
  ):
    try:
      return verify(password, stored)
    except ValueError:
      return false

  if verifyLegacyBcryptPassword(password, stored):
    return true

  result = verifyLegacyMd5Password(password, stored)
