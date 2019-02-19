{-# LANGUAGE MultiWayIf #-}

module Main where

import           Control.Exception     (IOException, bracket, try)
import           Control.Monad         (forM, forM_, unless, when)
import qualified Crypto.Hash.SHA1      as SHA1
import           Data.Bits             (shiftL, shiftR)
import qualified Data.ByteString       as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy  as BSL
import           Data.Int              (Int64)
import           Data.List             (find, foldl', sort, sortOn)
import           Data.Maybe            (catMaybes, fromJust, fromMaybe, isJust, isNothing)
import           Data.String           (fromString)
import           Data.Time.Clock.POSIX
import           Data.Word             (Word32)
import           Numeric               (showHex)
import           Options.Applicative   (Parser, ParserInfo, ReadM, argument, auto, execParser,
                                        footer, fullDesc, help, helper, info, long, many, metavar,
                                        option, progDesc, readerError, short, str, switch, value)
import           System.Directory      (canonicalizePath, listDirectory)
import           System.FilePath       (makeRelative, takeFileName, (<.>), (</>))
import           System.IO             (Handle, hGetEncoding, hPutStrLn, hSetEncoding,
                                        mkTextEncoding, stderr, stdin, stdout)
import           System.Posix.Files    (FileStatus, deviceID, fileID, fileMode, fileSize,
                                        getSymbolicLinkStatus, isDirectory, isRegularFile,
                                        isSymbolicLink, modificationTimeHiRes, readSymbolicLink,
                                        statusChangeTimeHiRes)
import           Text.Printf           (printf)

import           DB
import qualified Mnt
import           Stat                  (fileBlockSize)


-- * Options

data Options = Options { _optSHA1   :: Bool
                       , optHID     :: Bool
                       , _optSize   :: Bool
                       , optDU      :: Bool
                       , optTotal   :: Bool

                       , _optCLevel :: CacheLevel
                       , optMtime   :: Bool
                       , optSI      :: Bool
                       , optSort    :: Bool

                       , _optPaths  :: [FilePath]
                       }

optOutUnspecified :: Options -> Bool
optOutUnspecified opt = not (_optSHA1 opt || optHID opt || _optSize opt || optDU opt)

optSHA1 :: Options -> Bool
optSHA1 opt = optOutUnspecified opt || _optSHA1 opt

optSHA1' :: Options -> Bool -- whether sha1 must be computed
optSHA1' opt = optSHA1 opt || optHID opt

optSize :: Options -> Bool
optSize opt = optOutUnspecified opt || _optSize opt

optPaths :: Options -> [FilePath]
optPaths opt = if null (_optPaths opt) then ["."] else _optPaths opt

optCLevel :: Options -> CacheLevel
optCLevel opt | _optCLevel opt == -1 && optSHA1' opt       = 1
              | _optCLevel opt == -1 && not (optSHA1' opt) = 2
              | otherwise                                  = _optCLevel opt

parserOptions :: Parser Options
parserOptions = Options
                <$> switch (long "sha1" <> short 'x' <>
                            help "print sha1 hash (in hexadecimal) (DEFAULT)")
                <*> switch (long "hid" <> short '#' <>
                            help "print unique (system-wide) integer ID corresponding to sha1 hash")
                <*> switch (long "size" <> short 's' <>
                            help "print (apparent) size (DEFAULT)")
                <*> switch (long "disk-usage" <> short 'd' <>
                            help "print actual size (disk usage) (EXPERIMENTAL)")
                <*> switch (long "total" <> short 'c' <>
                            help "produce a grand total")

                <*> option clevel (long "cache-level" <> short 'l' <> value (-1) <> metavar "INT" <>
                                   help "policy for cache use, in 0..3 (default: 1 or 2)")
                <*> switch (long "mtime" <> short 'm' <>
                            help "use mtime instead of ctime to interpret cache level")
                <*> switch (long "si" <> short 't' <>
                            help "use powers of 1000 instead of 1024 for sizes")
                <*> switch (long "sort" <> short 'S' <>
                            help "sort output, according to size")

                <*> many (argument str (metavar "PATHS..." <> help "files or directories (default: \".\")"))

options :: ParserInfo Options
options = info (helper <*> parserOptions)
          $  fullDesc
          <> progDesc "compute and cache the sha1 hash and size of files and directories"
          <> footer "Cache level l: the cache (hash and size) is used if:                                      \
                    \ l ≥ 1 and the timestamp of a file is compatible,                                         \
                    \ l ≥ 2 and the timestamp of a directory is compatible,                                    \
                    \ or l = 3.                                                                                \
                    \ The timestamp is said \"compatible\" if ctime (or mtime with the -m option)              \
                    \ is older than the time of caching.                                                       \
                    \ When the size of a file has changed since cached, the hash is unconditionally            \
                    \ re-computed (even when l = 3).                                                           \
                    \ The default value of l is 1 when hashes are requested (should be reliable in most cases) \
                    \ and 2 when only the size is requested (this avoids to recursively traverse directories,  \
                    \ which would then be no better than du)."

type CacheLevel = Int

clevel :: ReadM CacheLevel
clevel = do
  i <- auto
  if 0 <= i && i <= 3
    then return i
    else readerError "cache level isn't in the range 0..3"


-- * main

main :: IO ()
main = do
  mapM_ mkTranslitEncoding [stdout, stderr, stdin]

  opt <- execParser options
  bracket newDB closeDB $ \db -> do

    let printEntry ent = do
          hid <- if optHID opt
                   then sequence $ getHID db <$> _hash ent
                   else return Nothing
          putStrLn . showEntry opt hid $ ent

    mps <- filter (isJust . Mnt.uuid) <$> Mnt.points

    list_ <- forM (mkEntry opt db mps <$> optPaths opt) $ \entIO_ -> do
      ent_ <- entIO_
      case ent_ of
        Nothing -> return Nothing
        Just ent -> do
          unless (optSort opt) (printEntry ent)
          return ent_

    let list = catMaybes list_
    when (optSort opt) $ forM_ (sortOn _size list) printEntry
    when (optTotal opt) $
      printEntry $ combine ("*total*", 0, True, 0, 0)
                           (sortOn (takeFileName . _path) list)


mkTranslitEncoding :: Handle -> IO ()
mkTranslitEncoding h =
  hGetEncoding h >>= mapM_ (\enc ->
    hSetEncoding h =<< mkTextEncoding (takeWhile (/= '/') (show enc) ++ "//TRANSLIT"))


-- * Entry

data Entry = Entry { _path   :: FilePath
                   , _mode   :: Word32
                   , _sizeOK :: Bool -- size and du are reliable
                   , _size   :: Int
                   , _du     :: Int
                   , _hash   :: Maybe BS.ByteString
                   } deriving (Show)


mkEntry :: Options -> DB -> [Mnt.Point] -> FilePath -> IO (Maybe Entry)
mkEntry opt db mps path = do
  status' <- try (getSymbolicLinkStatus path) :: IO (Either IOException FileStatus)
  now' <- getPOSIXTime
  case status' of
    Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                         return Nothing
    Right status -> do
      when (isNothing mp) $
        hPutStrLn stderr ("warning: mount point not found for device: " ++ show dev)
      key <- maybe (return NoKey) (mkKey status path) mp -- key won't be used when mp == Nothing
      if
       | isRegularFile status -> do
           let newent' = return . Just . Entry path mode True size du
               newent put = do
                 h' <- try $ sha1sum path :: IO (Either IOException BS.ByteString)
                 case h' of
                   Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                                        newent' Nothing
                   Right h        -> do _ <- put db dbpath $ mkDBEntry (key, now, size, du, h)
                                        newent' $ Just h
           if optSHA1' opt
             -- DB access is not lazy so a guard is needed somewhere to avoid computing the hash
             -- we can as well avoid the getDB call in this case, as a small optimization
             then do
               entry_ <- getDB db dbpath $ fromKey key
               case entry_ of
                 Nothing -> newent insertDB
                 Just (_, t, s, _, h, p)
                   | s /= size                           -> newent updateDB
                   | not $ keyMatches key p              -> newent updateDB
                   | (cl == 1 || cl == 2) && t >= cmtime -> newent' $ Just h
                   | cl == 3                             -> newent' $ Just h
                   | otherwise                           -> newent updateDB
             else newent' Nothing
       | isSymbolicLink status ->
           -- we compute hash conditionally as directories containing only symlinks will otherwise
           -- provoke its evaluation; putting instead the condition when storing dir infos into the DB
           -- seems the make the matter worse (requiring then to make the hash field strict...)
           Just . Entry path mode True size du <$>
             if optSHA1' opt then Just <$> sha1sumSymlink path
                             else return Nothing
       | isDirectory status -> do
           let newent put = do
                 files' <- try (listDirectory path) :: IO (Either IOException [FilePath])
                 case files' of
                   Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                                        return . Just $ Entry path mode False size du Nothing
                   Right files -> do
                     entries <- sequence $ mkEntry opt db mps . (path </>) <$> sort files :: IO [Maybe Entry]
                     let dir = combine (path, mode, True, size, du) $ catMaybes entries
                     when (_sizeOK dir) $
                       -- if the above condition is true, a read error occured somewhere and the info
                       -- can't reliably be stored into the DB
                       put db dbpath $ mkDBEntry (key, now, _size dir, _du dir, fromMaybe BS.empty (_hash dir))
                     return . Just $ dir
           let hcompat h = not (BS.null h && optSHA1' opt)
           entry_ <- getDB db dbpath $ fromKey key
           case entry_ of
             Nothing -> newent insertDB
             Just (_, t, s, d, h, p)
               | not $ keyMatches key p              -> newent updateDB
               | cl == 2 && t >= cmtime && hcompat h -> newent'
               | cl == 3 && hcompat h                -> newent'
               | otherwise                           -> newent updateDB
               where newent' = return . Just $ Entry path mode True s d (if BS.null h then Nothing else Just h)
       | otherwise -> return . Just . Entry path mode True size du . Just $ BS.pack $ replicate 20 0
       where mode   = fromIntegral $ fileMode status
             cmtime = ceiling $ 10^(9::Int) * (if optMtime opt then modificationTimeHiRes else statusChangeTimeHiRes) status
             now    = ceiling $ 10^(9::Int) * now'
             size   = fromIntegral $ fileSize status
             du     = fileBlockSize status * 512 -- TODO: check 512 is always valid
             dev    = fromIntegral $ deviceID status
             mp     = find ((== dev) . Mnt.devid) mps
             uuid   = mp >>= Mnt.uuid :: Maybe String
             dbpath = (\x -> "/var/cache/fh" </> x <.> "db") <$> uuid :: Maybe FilePath
             cl     = optCLevel opt


data Key = NoKey | Inode Int64 | PathHash Int64 BS.ByteString

mkKey :: FileStatus -> FilePath -> Mnt.Point -> IO Key
mkKey status path mp =
  if Mnt.fstype mp `elem` ["ext2", "ext3", "ext4"]
  then return . Inode . fromIntegral . fileID $ status
  else do
    realpath <- canonicalizePath path
    let hash = SHA1.hash . fromString $ makeRelative (Mnt.target mp) realpath
        h64  = fromIntegral <$> BS.unpack hash :: [Int64]
    return $ PathHash (sum [w `shiftL` i | (w, i) <- zip h64 [0, 8..]]) hash

fromKey :: Key -> Int64
fromKey (Inode k)      = k
fromKey (PathHash k _) = k
fromKey NoKey          = error "invalid key"

mkDBEntry :: (Key, Int64, Int, Int, BS.ByteString) -> DBEntry
mkDBEntry (Inode k, n, s, d, h)      = (k, n, s, d, h, BS.empty)
mkDBEntry (PathHash k p, n, s, d, h) = (k, n, s, d, h, p)
mkDBEntry _                          = error "invalid key"

keyMatches :: Key -> BS.ByteString -> Bool
keyMatches (Inode _) p | BS.null p = True
                       | otherwise = error "unexpected key"
keyMatches (PathHash _ p) p' | p == p' = True
                             | otherwise = False
keyMatches NoKey _ = error "invalid key"

-- the 1st param gives non-cumulated (own) size and other data for resulting Entry
combine :: (String, Word32, Bool, Int, Int) -> [Entry] -> Entry
combine (name, mode, ok0, s0, d0) entries = finalize $ foldl' update (ok0, s0, d0, pure dirCtx) entries
  where update (ok, s, d, ctx) (Entry n m ok' size du hash) =
          (ok && ok', s+size, d+du, SHA1.update <$> ctx <*>
              fmap BS.concat (sequence [hash, Just $ pack32 m, Just $ Char8.pack $ takeFileName n, Just $ BS.singleton 0]))
              -- The mode of a file has no effect on its hash, only on its containing dir's hash;
              -- this is similar to how git does.
              -- We add '\0' (`singleton 0`) to be sure that 2 unequal dirs won't have the same hash;
              -- this assumes '\0' can't be in a file name.
        finalize (ok, s, d, ctx) = Entry name mode ok s d $ SHA1.finalize <$> ctx
        pack32 m = BS.pack $ fromIntegral <$> [m, m `shiftR` 8, m `shiftR` 16, m `shiftR` 24]


-- * display

showEntry :: Options -> Maybe Int64 -> Entry -> String
showEntry opt hid (Entry path _ _ size du hash) =
  let sp   = if optSize opt then formatSize opt size ++ "  " ++ path  else path
      dsp  = if optDU opt then formatSize opt du ++ "  " ++ sp        else sp
      hdsp = if optSHA1 opt -- if hash is still Nothing, there was an I/O error
             then maybe (replicate 40 '*') hexlify hash ++ "  " ++ dsp else dsp
      in formatHashID opt hid ++ hdsp

formatHashID :: Options -> Maybe Int64 -> String
formatHashID opt hid =
  if optHID opt
  then let s = "#" ++ maybe "*" show hid
       in printf "%5s  " s
  else ""

formatSize :: Options -> Int -> String
formatSize opt sI =
  let u0:units = (if optSI opt then (++"B") else id) <$>
                 [" ", "k", "M", "G", "T", "P", "E", "Z", "Y"]
      base = if optSI opt then 1000.0 else 1024.0
      (s', u) = fromJust $ find (\(s, _) -> s < 1000.0) $
                  scanl (\(s, _) f' ->  (s / base, f')) (fromIntegral sI :: Double, u0) units
  in printf (if head u == ' ' then "%3.f  %s" else "%5.1f%s") s' u


-- * sha1 hash

-- random seed for dirs
dirCtx :: SHA1.Ctx
dirCtx = SHA1.update SHA1.init $ BS.pack [0x2a, 0xc9, 0xd8, 0x3b, 0xc8, 0x7c, 0xe4, 0x86, 0xb2, 0x41,
                                          0xd2, 0x27, 0xb4, 0x06, 0x93, 0x60, 0xc6, 0x2b, 0x52, 0x37]

-- random seed for symlinks
symlinkCtx :: SHA1.Ctx
symlinkCtx = SHA1.update SHA1.init $ BS.pack [0x05, 0xfe, 0x0d, 0x17, 0xac, 0x9a, 0x10, 0xbc, 0x7d, 0xb1,
                                              0x73, 0x99, 0xa6, 0xea, 0x92, 0x38, 0xfa, 0xda, 0x0f, 0x16]

sha1sumSymlink :: FilePath -> IO BS.ByteString
sha1sumSymlink path = SHA1.finalize . SHA1.update symlinkCtx . fromString <$> readSymbolicLink path

sha1sum :: FilePath -> IO BS.ByteString
sha1sum = fmap SHA1.hashlazy . BSL.readFile

hexlify :: BS.ByteString -> String
hexlify bstr = let words8 = BS.unpack bstr
                   showHexPad x = if x >= 16 then showHex x else showChar '0' . showHex x
                   hex = map showHexPad words8
               in foldr ($) "" hex
