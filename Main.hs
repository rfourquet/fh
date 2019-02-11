module Main where

import           Control.Exception     (SomeException, try)
import           Control.Monad         (forM_, when)
import qualified Crypto.Hash.SHA1      as SHA1
import           Data.Bits             (shiftR)
import qualified Data.ByteString       as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy  as BSL
import           Data.Int              (Int64)
import           Data.List             (find, foldl', sort, sortOn)
import           Data.Maybe            (catMaybes, fromJust, fromMaybe)
import           Data.String           (fromString)
import           Data.Word             (Word32)
import           Numeric               (showHex)
import           Options.Applicative   (Parser, ParserInfo, argument, execParser, fullDesc, help,
                                        helper, info, long, many, metavar, progDesc, short, str,
                                        switch)
import           System.Directory      (listDirectory)
import           System.FilePath       (takeFileName, (<.>), (</>))
import           System.IO             (hPutStrLn, stderr)
import           System.Posix.Files    (FileStatus, deviceID, fileID, fileMode, fileSize,
                                        getSymbolicLinkStatus, isDirectory, isRegularFile,
                                        isSymbolicLink, readSymbolicLink, statusChangeTimeHiRes)
import           Text.Printf           (printf)

import           DB
import qualified Mnt
import           Stat                  (fileBlockSize)


-- * Options

data Options = Options { _optSHA1  :: Bool
                       , _optSize  :: Bool
                       , optDU     :: Bool
                       , optTotal  :: Bool

                       , optSI     :: Bool
                       , optSort   :: Bool

                       , _optPaths :: [FilePath]
                       }

optOutUnspecified :: Options -> Bool
optOutUnspecified opt = not (_optSHA1 opt || _optSize opt || optDU opt)

optSHA1 :: Options -> Bool
optSHA1 opt = optOutUnspecified opt || _optSHA1 opt

optSize :: Options -> Bool
optSize opt = optOutUnspecified opt || _optSize opt

optPaths :: Options -> [FilePath]
optPaths opt = if null (_optPaths opt) then ["."] else _optPaths opt

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

                <*> many (argument str (metavar "PATHS..." <> help "files or directories (default: \".\")"))

options :: ParserInfo Options
options = info (helper <*> parserOptions)
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
    putStrLn $ showEntry opt $ combine ("*total*", 0, 0, 0, 0, 0)
                                       (sortOn (takeFileName . _path) list)
  closeDB db


-- * Entry

data Entry = Entry { _path  :: FilePath
                   , _mode  :: Word32
                   , _key   :: Int64
                   , _ctime :: Int64
                   , _size  :: Int
                   , _du    :: Int
                   , _hash  :: Maybe BS.ByteString
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
          let newent = return . Just . Entry path mode key ctime size du
          entry_ <- getDB db dbpath key
          case entry_ of
            Nothing -> if optSHA1 opt then do -- necessary to compute the hash conditionally
                         h <- sha1sum path
                         insertDB db dbpath (key, ctime, size, du, h)
                         newent $ Just h
                       else newent Nothing
            Just (_, _, _, _, h) -> newent $ Just h
      | isSymbolicLink status ->
          Just . Entry path mode key ctime size du . Just . SHA1.hash . fromString <$>
            readSymbolicLink path
      | isDirectory status -> do
          let newent put = do
                files <- listDirectory path
                entries <- sequence $ mkEntry opt db mps . (path </>) <$> sort files :: IO [Maybe Entry]
                let dir = combine (path, mode, key, ctime, size, du) $ catMaybes entries
                _ <- put db dbpath (key, ctime, _size dir, _du dir, fromMaybe BS.empty (_hash dir))
                return . Just $ dir
          entry_ <- getDB db dbpath key
          case entry_ of
            Nothing -> newent insertDB
            Just (_, _, s, d, h) ->
              if optSHA1 opt && h == BS.empty
              then newent updateDB
              else return . Just $ Entry path mode key ctime s d (if BS.null h then Nothing else Just h)
      | otherwise -> return Nothing
      where key    = fromIntegral $ fileID status
            dev    = fromIntegral $ deviceID status
            mode   = fromIntegral $ fileMode status
            ctime  = ceiling $ 10^(9::Int) * statusChangeTimeHiRes status
            size   = fromIntegral $ fileSize status
            du     = fileBlockSize status * 512 -- TODO: check 512 is always valid
            uuid   = find ((== dev) . Mnt.devid) mps >>= Mnt.uuid :: Maybe String
            dbpath = (\x -> "/var/cache/fh" </> x <.> "db") <$> uuid :: Maybe FilePath


-- the 1st param gives non-cumulated (own) size and other data for resulting Entry
combine :: (String, Word32, Int64, Int64, Int, Int) -> [Entry] -> Entry
combine (name, mode, key, ctime, s0, d0) entries = finalize $ foldl' update (s0, d0, pure SHA1.init) entries
  where update (s, d, ctx) (Entry n m _ _ size du hash) =
          (s+size, d+du, SHA1.update <$> ctx <*>
              fmap BS.concat (sequence [hash, Just $ pack32 m, Just $ Char8.pack $ takeFileName n, Just $ BS.singleton 0]))
              -- The mode of a file has no effect on its hash, only on its containing dir's hash;
              -- this is similar to how git does.
              -- We add '\0' (`singleton 0`) to be sure that 2 unequal dirs won't have the same hash;
              -- this assumes '\0' can't be in a file name.
        finalize (s, d, ctx) = Entry name mode key ctime s d $ SHA1.finalize <$> ctx
        pack32 m = BS.pack $ fromIntegral <$> [m, m `shiftR` 8, m `shiftR` 16, m `shiftR` 24]


showEntry :: Options -> Entry -> String
showEntry opt (Entry path _ _ _ size du hash) =
  let sp = if optSize opt then formatSize opt size ++ "  " ++ path else path
      dsp = if optDU opt then formatSize opt du ++ "  " ++ sp else sp
  in if optSHA1 opt then hexlify (fromJust hash) ++ "  " ++ dsp else dsp

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
