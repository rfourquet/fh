{-# LANGUAGE OverloadedStrings #-}

module Hashing (dirCtx, getAnnexSizeAndHash, hexlify, sha1sum, sha1sumSymlink, unhexlify') where

import           Control.Applicative              (many, (<|>))
import           Control.Monad                    (when)
import qualified Crypto.Hash.SHA1                 as SHA1
import           Data.Attoparsec.ByteString       (Parser, count, endOfInput, inClass, match,
                                                   option, parseOnly, satisfy, skipMany1, string,
                                                   word8)
import           Data.Attoparsec.ByteString.Char8 (char8, decimal)
import           Data.Bits                        (shiftL, (.|.))
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString                  as B
import           Data.Functor                     (void)
import           Data.Word                        (Word8)
import           Numeric                          (showHex)
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

-- assumes a valid hex string
unhexlify' :: ByteString -> ByteString
unhexlify' h = B.pack . fst $ B.foldr' f ([], Nothing) h
  where f w (ws, carry) =
            case carry of
              Nothing -> (ws, Just $ val w)
              Just v  -> ((v .|. val w `shiftL` 4):ws, Nothing)
        val w = if w <= 57 then w - 48 else w - 87


-- * Annex

getAnnexSizeAndHash :: Bool -> RawFilePath -> Maybe (Int, ByteString)
getAnnexSizeAndHash isLink path =
    case parsed of
      Left _                 -> Nothing
      Right (AnnexKey s h _) -> Just (s, h)
  where parsed = if isLink
        then parseOnly annexLinkP path
        else parseOnly annexFileP $ R.takeFileName path

data AnnexKey = AnnexKey { _size :: Int
                         , _hash :: ByteString
                         , _ext  :: Bool
                         } deriving Show

annexKeyP :: Parser AnnexKey
annexKeyP = do
    algo <- string "SHA1E" <|> string "SHA1"
    dash
    char8_ 's'
    s <- decimal
    dash >> dash
    (h, _) <- match $ count 40 hex
    return $ AnnexKey s (unhexlify' h) (algo == "SHA1E")
  where dash = char8_ '-'
        hex = satisfy $ inClass "0-9a-f"

annexExtP :: AnnexKey -> Parser AnnexKey
annexExtP key = do
    when (_ext key) $ option () $ do
      char8_ '.'
      skipMany1 $ satisfy (/= R.pathSeparator) -- git-annex probably allows only 3 or 4 chars as extension
    endOfInput
    return key

annexFileP :: Parser AnnexKey
annexFileP = annexKeyP >>= annexExtP

annexLinkP :: Parser AnnexKey
annexLinkP = do
    void $ many upDir
    void $ string ".git/annex/objects/"
    subDir >> subDir
    (ks, key) <- match annexKeyP
    pathSep
    void $ string ks
    annexExtP key
  where upDir = string "../"
        isAlphaNum = satisfy $ inClass "0-9a-zA-Z"
        subDir = void $ isAlphaNum >> isAlphaNum >> pathSep
        pathSep = void $ word8 R.pathSeparator

char8_ :: Char -> Parser ()
char8_ = void . char8
