module Main where

import           Control.Exception    (SomeException, try)
import           Control.Monad        (forM, when)
import qualified Crypto.Hash.SHA1     as SHA1
import qualified Data.ByteString      as BS
import qualified Data.ByteString.Lazy as BSL
import           Data.List            (foldl', sort)
import           Data.Maybe           (catMaybes)
import           Data.String          (fromString)
import           Numeric              (showHex)
import           Options.Applicative  (Parser, ParserInfo, argument, execParser,
                                       fullDesc, help, helper, info, long, many,
                                       metavar, progDesc, short, str, switch,
                                       (<**>))
import           System.Directory     (listDirectory)
import           System.FilePath      ((</>))
import           System.IO            (hPutStrLn, stderr)
import           System.Posix.Files   (FileStatus, fileSize,
                                       getSymbolicLinkStatus, isDirectory,
                                       isRegularFile, isSymbolicLink,
                                       readSymbolicLink)
import           Text.Printf          (printf)


-- * Options

data Options = Options { _optSHA1 :: Bool
                       , _optSize :: Bool
                       , optTotal :: Bool

                       , optSI    :: Bool
                       , optSort  :: Bool

                       , optPaths :: [FilePath]
                       }

optOutUnspecified :: Options -> Bool
optOutUnspecified opt = not (_optSHA1 opt || _optSize opt)

optSHA1 :: Options -> Bool
optSHA1 opt = optOutUnspecified opt || _optSHA1 opt

optSize :: Options -> Bool
optSize opt = optOutUnspecified opt || _optSize opt

parserOptions :: Parser Options
parserOptions = Options
                <$> switch (long "sha1" <> short 'x' <>
                            help "print sha1 hash (in hexadecimal)")
                <*> switch (long "size" <> short 's' <>
                            help "print (apparent) size")
                <*> switch (long "total" <> short 'c' <>
                            help "produce a grand total")

                <*> switch (long "si" <> short 't' <>
                            help "use powers of 1000 instead of 1024 for sizes")
                <*> switch (long "sort" <> short 'S' <>
                            help "sort output, according to size")

                <*> many (argument str (metavar "PATHS..."))

options :: ParserInfo Options
options = info (parserOptions <**> helper)
          $  fullDesc
          <> progDesc "compute and cache the sha1 hash and size of files and directories"


-- * main

main :: IO ()
main = do
  opt <- execParser options
  list' <- forM (optPaths opt) $ \path -> do
    ent' <- mkEntry opt path
    case ent' of
      Nothing  -> return Nothing
      Just ent -> do putStrLn $ showEntry opt ent
                     return ent'
  let list = catMaybes list'
  when (optTotal opt) $
    putStrLn $ showEntry opt $ combine opt ("*total*", 0) list


-- * Entry

data Entry = Entry { _path :: FilePath
                   , _size :: Int
                   , _hash :: BS.ByteString
                   } deriving (Show)


mkEntry :: Options -> FilePath -> IO (Maybe Entry)
mkEntry opt path = do
  status' <- try (getSymbolicLinkStatus path) :: IO (Either SomeException FileStatus)
  case status' of
    Left exception -> do
      hPutStrLn stderr $ "error: " ++ show exception
      return Nothing
    Right status
      | isRegularFile status ->
          Just . Entry path size <$> sha1sum path
      | isSymbolicLink status ->
          Just . Entry path size . SHA1.hash . fromString <$> readSymbolicLink path
      | isDirectory status -> do
          files <- listDirectory path
          entries <- sequence $ mkEntry opt . (path </>) <$> sort files :: IO [Maybe Entry]
          return $ Just $ combine (path, size) $ catMaybes entries
      | otherwise -> return Nothing
      where size = fromIntegral $ fileSize status


-- (name, s0) are "path" and non-cumulated (own) size for resulting Entry
combine :: (String, Int) -> [Entry] -> Entry
combine (name, s0) entries = finalize $ foldl' update (s0, SHA1.init) entries
  where update (s, ctx) (Entry _ size hash) = (s+size, SHA1.update ctx hash)
        finalize (s, ctx) = Entry name s $ SHA1.finalize ctx


showEntry :: Options -> Entry -> String
showEntry opt (Entry path size hash) =
  let sp = if optSize opt then formatSize opt size ++ "  " ++ path else path
  in if optSHA1 opt then hexlify hash ++ "  " ++ sp else sp

formatSize :: Options -> Int -> String
formatSize opt sI =
  let u0:units = (if optSI opt then (++"B") else id) <$>
                 [" ", "k", "M", "G", "T", "P", "E", "Z", "Y"]
      base = if optSI opt then 1000.0 else 1024.0
      (s', u) = head $ dropWhile (\(s, _) -> s >= 1000.0) $
                         scanl (\(s, _) f' ->  (s / base, f')) (fromIntegral sI :: Double, u0) units
  in printf (if head u == ' ' then "%3.f  %s" else "%5.1f%s") s' u


-- * sha1 hash

sha1sum :: FilePath -> IO BS.ByteString
sha1sum = fmap SHA1.hashlazy . BSL.readFile

hexlify :: BS.ByteString -> String
hexlify bstr = let words8 = BS.unpack bstr
                   showHexPad x = if x >= 16 then showHex x else showChar '0' . showHex x
                   hex = map showHexPad words8
               in foldr ($) "" hex
