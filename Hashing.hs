{-# LANGUAGE OverloadedStrings #-}

module Hashing (dirCtx, getAnnexSizeAndHash, hexlify, sha1sum, sha1sumSymlink) where

import           Control.Applicative              ((<|>))
import           Control.Monad                    (when)
import qualified Crypto.Hash.SHA1                 as SHA1
import           Data.Attoparsec.ByteString       (Parser, anyWord8, count, endOfInput, inClass,
                                                   match, parseOnly, satisfy, skipMany1, string)
import           Data.Attoparsec.ByteString.Char8 (char8, decimal)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString                  as B
import qualified Data.ByteString.Char8            as BC
import           Data.Functor                     (void)
import           Data.List.Split                  (chunksOf)
import           Data.Word                        (Word8)
import           Numeric                          (readHex, showHex)
import           System.Posix.ByteString          (RawFilePath)

import qualified RawPath                          as R


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
getAnnexSizeAndHash path =
  case parseOnly annexKeyP $ R.takeFileName path of
    Left _               -> Nothing
    Right (AnnexKey s h) -> (,) <$> Just s <*> unhexlify (BC.unpack h)

type HexString = ByteString

data AnnexKey = AnnexKey { _size :: Int
                         , _hash :: HexString
                         } deriving Show

annexKeyP :: Parser AnnexKey
annexKeyP = do
    algo <- string "SHA1E" <|> string "SHA1"
    dash
    char8' 's'
    s <- decimal
    dash >> dash
    (h, _) <- match $ count 40 hex
    when (algo == "SHA1E") $ do
      char8' '.'
      skipMany1 anyWord8 -- git-annex probably allows only 3 or 4 chars as extension
    endOfInput
    return $ AnnexKey s h
  where dash = char8' '-'
        hex = satisfy $ inClass "0-9a-f"
        char8' = void . char8 -- to suppress "unused-do-bind" warnings
