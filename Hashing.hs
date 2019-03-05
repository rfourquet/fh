module Hashing (dirCtx, getAnnexSizeAndHash, hexlify, sha1sum, sha1sumSymlink) where

import qualified Crypto.Hash.SHA1        as SHA1
import           Data.ByteString         (ByteString)
import qualified Data.ByteString         as B
import qualified Data.ByteString.Char8   as BC
import           Data.List.Split         (chunksOf, splitOn)
import           Data.Word               (Word8)
import           Numeric                 (readHex, showHex)
import qualified RawPath                 as R
import           System.FilePath         (takeBaseName)
import           System.Posix.ByteString (RawFilePath)
import           Text.Read               (readMaybe)


-- random seed for dirs
dirCtx :: SHA1.Ctx
dirCtx = SHA1.update SHA1.init $ B.pack [0x2a, 0xc9, 0xd8, 0x3b, 0xc8, 0x7c, 0xe4, 0x86, 0xb2, 0x41,
                                         0xd2, 0x27, 0xb4, 0x06, 0x93, 0x60, 0xc6, 0x2b, 0x52, 0x37]

-- random seed for symlinks
symlinkCtx :: SHA1.Ctx
symlinkCtx = SHA1.update SHA1.init $ B.pack [0x05, 0xfe, 0x0d, 0x17, 0xac, 0x9a, 0x10, 0xbc, 0x7d, 0xb1,
                                             0x73, 0x99, 0xa6, 0xea, 0x92, 0x38, 0xfa, 0xda, 0x0f, 0x16]

sha1sumSymlink :: RawFilePath -> ByteString
sha1sumSymlink = SHA1.finalize . SHA1.update symlinkCtx

sha1sum :: RawFilePath -> IO ByteString
sha1sum = fmap SHA1.hashlazy . R.readFileLazy

-- TODO: check out Data.ByteString.Base16 to replace hexlify & unhexlify

hexlify :: ByteString -> String
hexlify bstr = let ws = B.unpack bstr :: [Word8]
                   showHexPad x = if x >= 16 then showHex x else showChar '0' . showHex x
                   hex = showHexPad <$> ws
               in foldr ($) "" hex

unhexlify :: String -> Maybe ByteString
unhexlify h = let ws' = concatMap readHex $ chunksOf 2 h :: [(Word8, String)]
                  valid = all (null . snd) ws'
                  ws  = fst <$> ws'
              in if valid then Just $ B.pack ws else Nothing

getAnnexSizeAndHash :: RawFilePath -> Maybe (Int, ByteString)
getAnnexSizeAndHash path' =
  let path = BC.unpack path'
      parts = splitOn "-" $ takeBaseName path

  in case parts of
     [backend, size, "", hex]
       | backend `elem` ["SHA1", "SHA1E"] && head size == 's' && length hex == 40 ->
           (,) <$> readMaybe (tail size) <*> unhexlify hex
       | otherwise -> Nothing
     _ -> Nothing
