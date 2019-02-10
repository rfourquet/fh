module Main where

import           Control.Exception    (SomeException, try)
import           Control.Monad        (forM_, when)
import qualified Crypto.Hash.SHA1     as SHA1
import qualified Data.ByteString      as BS
import qualified Data.ByteString.Lazy as BSL
import           Data.Int             (Int64)
import           Data.List            (find, foldl', sort, sortOn)
import           Data.Maybe           (catMaybes)
import           Data.String          (fromString)
import           Numeric              (showHex)
import           Options.Applicative  (Parser, ParserInfo, argument, execParser,
                                       fullDesc, help, helper, info, long, many,
                                       metavar, progDesc, short, str, switch,
                                       (<**>))
import           System.Directory     (listDirectory)
import           System.FilePath      ((<.>), (</>))
import           System.IO            (hPutStrLn, stderr)
import           System.Posix.Files   (FileStatus, deviceID, fileID, fileSize,
                                       getSymbolicLinkStatus, isDirectory,
                                       isRegularFile, isSymbolicLink,
                                       readSymbolicLink, statusChangeTimeHiRes)
import           Text.Printf          (printf)

import           DB
import qualified Mnt
import           Stat                 (fileBlockSize)


-- * Options

data Options = Options { _optSHA1 :: Bool
                       , _optSize :: Bool
                       , optDU    :: Bool
                       , optTotal :: Bool

                       , optSI    :: Bool
                       , optSort  :: Bool

                       , optPaths :: [FilePath]
                       }

optOutUnspecified :: Options -> Bool
optOutUnspecified opt = not (_optSHA1 opt || _optSize opt || optDU opt)

optSHA1 :: Options -> Bool
optSHA1 opt = optOutUnspecified opt || _optSHA1 opt

optSize :: Options -> Bool
optSize opt = optOutUnspecified opt || _optSize opt

parserOptions :: Parser Options
parserOptions = Options
                <$> switch (long "sha1" <> short 'x' <>
                            help "print sha1 hash (in hexadecimal) (DEFAULT)")
                <*> switch (long "size" <> short 's' <>
                            help "print (apparent) size (DEFAULT)")
                <*> switch (long "disk-usage" <> short 'd' <>
                            help "print actual size (disk usage) (EXPERIMENTAL)")
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
  db <- newDB
  mps <- Mnt.points
  list <- (if optSort opt then sortOn _size else id) . catMaybes <$>
            sequence (mkEntry opt db mps <$> optPaths opt)
  forM_ list $ putStrLn . showEntry opt
  when (optTotal opt) $
    putStrLn $ showEntry opt $ combine ("*total*", 0, 0, 0, 0, 0) list
  closeDB db


-- * Entry

data Entry = Entry { _path  :: FilePath
                   , _devid :: Int64
                   , _key   :: Int64
                   , _ctime :: Int64
                   , _size  :: Int
                   , _du    :: Int
                   , _hash  :: BS.ByteString
                   } deriving (Show)


mkEntry :: Options -> DB -> [Mnt.Point] -> FilePath -> IO (Maybe Entry)
mkEntry opt db mps path = do
  status' <- try (getSymbolicLinkStatus path) :: IO (Either SomeException FileStatus)
  case status' of
    Left exception -> do
      hPutStrLn stderr $ "error: " ++ show exception
      return Nothing
    Right status
      | isRegularFile status -> do
          let newent = Just . Entry path dev key ctime size du
          entry_ <- getDB db dbpath key
          case entry_ of
            Nothing -> do h <- sha1sum path
                          insertDB db dbpath (key, ctime, size, du, h)
                          return $ newent h
            Just (_, _, _, _, h) -> return $ newent h
      | isSymbolicLink status ->
          Just . Entry path dev key ctime size du . SHA1.hash . fromString <$>
            readSymbolicLink path
      | isDirectory status -> do
          files <- listDirectory path
          entries <- sequence $ mkEntry opt db mps . (path </>) <$> sort files :: IO [Maybe Entry]
          return $ Just $ combine (path, dev, key, ctime, size, du) $ catMaybes entries
      | otherwise -> return Nothing
      where key    = fromIntegral $ fileID status
            dev    = fromIntegral $ deviceID status
            ctime  = ceiling $ 10^(9::Int) * statusChangeTimeHiRes status
            size   = fromIntegral $ fileSize status
            du     = fileBlockSize status * 512 -- TODO: check 512 is always valid
            uuid   = find ((== dev) . Mnt.devid) mps >>= Mnt.uuid :: Maybe String
            dbpath = (\x -> "/var/cache/fh" </> x <.> "db") <$> uuid :: Maybe FilePath


-- the 1st param gives non-cumulated (own) size and other data for resulting Entry
combine :: (String, Int64, Int64, Int64, Int, Int) -> [Entry] -> Entry
combine (name, dev, key, ctime, s0, d0) entries = finalize $ foldl' update (s0, d0, SHA1.init) entries
  where update (s, d, ctx) (Entry _ _ _ _ size du hash) = (s+size, d+du, SHA1.update ctx hash)
        finalize (s, d, ctx) = Entry name dev key ctime s d $ SHA1.finalize ctx


showEntry :: Options -> Entry -> String
showEntry opt (Entry path _ _ _ size du hash) =
  let sp = if optSize opt then formatSize opt size ++ "  " ++ path else path
      dsp = if optDU opt then formatSize opt du ++ "  " ++ sp else sp
  in if optSHA1 opt then hexlify hash ++ "  " ++ dsp else dsp

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
