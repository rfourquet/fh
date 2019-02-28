{-# LANGUAGE MultiWayIf #-}

module Main where

import           Control.Exception     (IOException, bracket, try)
import           Control.Monad         (forM_, unless, when)
import qualified Crypto.Hash.SHA1      as SHA1
import           Data.Bits             (complement, shiftL, shiftR, (.&.), (.|.))
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy  as BL
import           Data.Char             (ord)
import           Data.Function         ((&))
import           Data.Int              (Int64)
import           Data.IORef
import           Data.List             (find, foldl', isInfixOf, sortOn)
import           Data.List.Split       (chunksOf, splitOn)
import           Data.Maybe            (catMaybes, fromJust, fromMaybe, isJust)
import           Data.Set              (Set)
import qualified Data.Set              as Set
import           Data.Time.Clock.POSIX
import           Data.Word             (Word8)
import           Numeric               (readHex, showHex)
import           Options.Applicative   (Parser, ParserInfo, ReadM, argument, auto, columns,
                                        customExecParser, flag', fullDesc, help, helper, info, long,
                                        many, metavar, option, prefs, progDesc, readerError, short,
                                        str, strOption, switch, value)
import qualified Streaming.Prelude     as S
import           System.Directory      (canonicalizePath, listDirectory)
import           System.FilePath       (makeRelative, takeBaseName, takeDirectory, takeFileName,
                                        (</>))
import           System.IO             (Handle, hGetEncoding, hPutStrLn, hSetEncoding,
                                        mkTextEncoding, stderr, stdin, stdout)
import           System.Posix.Files    (FileStatus, accessModes, deviceID, directoryMode, fileID,
                                        fileMode, fileSize, getSymbolicLinkStatus,
                                        intersectFileModes, isDirectory, isRegularFile,
                                        isSymbolicLink, modificationTimeHiRes, readSymbolicLink,
                                        statusChangeTimeHiRes)
import           System.Posix.Types    (DeviceID, FileID, FileMode)
import           Text.Printf           (printf)

import           DB
import           Stat                  (fileBlockSize)


-- * Options

data Options = Options { optHelp     :: Bool

                       , _optSHA1    :: Bool
                       , _optHID     :: Int
                       , _optSize    :: Bool
                       , optDU       :: Bool
                       , optCnt      :: Bool
                       , optTotal    :: Bool

                       , _optCLevel  :: CacheLevel
                       , optMtime    :: Bool
                       , optSLink    :: Bool
                       , optALink    :: Bool
                       , optUnique   :: Bool
                       , optATrust   :: Bool
                       , optUseModes :: Bool
                       , optSI       :: Bool
                       , optMinSize  :: Int
                       , optMinCnt   :: Int
                       , _optSortS   :: Bool
                       , _optSortD   :: Bool
                       , optSortCnt  :: Bool
                       , optNoGit    :: Bool
                       , optInitDB   :: FilePath
                       , _optPaths   :: [FilePath]
                       }

optOutUnspecified :: Options -> Bool
optOutUnspecified opt = not . or $ [_optSHA1, optHID, _optSize, optDU, optCnt] <*> pure opt

optSHA1 :: Options -> Bool
optSHA1 opt = optOutUnspecified opt || _optSHA1 opt

optSHA1' :: Options -> Bool -- whether sha1 must be computed
optSHA1' opt = optSHA1 opt || optHID opt

optHID :: Options -> Bool
optHID = (> 0) . _optHID

optSize :: Options -> Bool
optSize opt = optOutUnspecified opt || _optSize opt

optSortS, optSortD :: Options -> Bool
optSortS opt = _optSortS opt && not (optSortCnt opt || _optSortD opt)
optSortD opt = _optSortD opt && not (optSortCnt opt)

optPaths :: Options -> [FilePath]
optPaths opt | null (_optPaths opt) = ["."]
             | optNoGit opt         = filter ((/= ".git") . takeFileName) $ _optPaths opt
             | otherwise            = _optPaths opt

optCLevel :: Options -> CacheLevel
optCLevel opt | _optCLevel opt == -1 && (optSHA1' opt || optUnique opt) = 1
              | _optCLevel opt == -1                                    = 2
              | otherwise                                               = _optCLevel opt

parserOptions :: Parser Options
parserOptions = Options
                <$> switch (long "long-help" <> short '?' <>
                            help "show help for --cache-level and --init-db options")
                <*> switch (long "sha1" <> short 'x' <>
                            help "print sha1 hash (in hexadecimal) (DEFAULT)")
                <*> (length <$> many (flag' () $
                       long "hid" <> short '#' <>
                       help "print unique (system-wide) integer ID corresponding to sha1 hash (use twice to reset the counter)"))
                <*> switch (long "size" <> short 's' <>
                            help "print (apparent) size (DEFAULT)")
                <*> switch (long "disk-usage" <> short 'd' <>
                            help "print actual size (disk usage) (EXPERIMENTAL)")
                <*> switch (long "count" <> short 'n' <>
                            help "print number of (recursively) contained files")
                <*> switch (long "total" <> short 'c' <>
                            help "produce a grand total")

                <*> option clevel (long "cache-level" <> short 'l' <> value (-1) <> metavar "INT" <>
                                   help "policy for cache use, in 0..3 (default: 1 or 2)")
                <*> switch (long "mtime" <> short 'm' <>
                            help "use mtime instead of ctime to interpret cache level")
                <*> switch (long "dereference" <> short 'L' <>
                            help "dereference all symbolic links")
                <*> switch (long "deref-annex" <> short 'A' <>
                            help "dereference all git-annex symbolic links")
                <*> switch (long "unique" <> short 'u' <>
                            help "discard files which have already been accounted for")
                <*> switch (long "trust-annex" <> short 'X' <>
                            help "trust the SHA1 hash encoded in a git-annex file name")
                <*> switch (long "use-modes" <> short 'M' <>
                            help "use file modes to compute the hash of directories")
                <*> switch (long "si" <> short 't' <>
                            help "use powers of 1000 instead of 1024 for sizes")
                <*> option auto (long "minsize" <> short 'z' <> value 0 <> metavar "INT" <>
                                 help "smallest size to show, in MiB")
                <*> option auto (long "mincount" <> short 'k' <> value 0 <> metavar "INT" <>
                                 help "smallest count to show")
                -- TODO make the 3 sorting options mutually exclusive
                <*> switch (long "sort" <> short 'S' <>
                            help "sort output, according to size")
                <*> switch (long "sort-du" <> short 'D' <>
                            help "sort output, according to disk usage")
                <*> switch (long "sort-count" <> short 'N' <>
                            help "sort output, according to count")
                <*> switch (long "ignore-git" <> short 'G' <>
                            help "ignore \".git\" filenames passed on the command line")
                <*> strOption (long "init-db" <> metavar "PATH" <> value "" <>
                               help "create a DB directory at PATH and exit")
                <*> many (argument str (metavar "PATHS..." <> help "files or directories (default: \".\")"))


options :: ParserInfo Options
options = info (helper <*> parserOptions)
          $  fullDesc
          <> progDesc "compute and cache the sha1 hash and size of files and directories"

type CacheLevel = Int

clevel :: ReadM CacheLevel
clevel = do
  i <- auto
  if 0 <= i && i <= 3
    then return i
    else readerError "cache level isn't in the range 0..3"

printHelp :: IO ()
printHelp = putStrLn
  " --cache-level L: the cache (hash and size) is used if:                       \n\
  \   * L ≥ 1 and the timestamp of a file is compatible,                         \n\
  \   * L ≥ 2 and the timestamp of a directory is compatible, or                 \n\
  \   * L = 3.                                                                   \n\
  \ The timestamp is said \"compatible\" if ctime (or mtime with the -m option)  \n\
  \ is older than the time of caching.                                           \n\
  \ When the size of a file has changed since cached, the hash is unconditionally\n\
  \ re-computed (even when L = 3).                                               \n\
  \ The default value of L is                                                    \n\
  \   * 1 when hashes are requested (should be reliable in most cases)           \n\
  \       or when the unique option is active (it's otherwise easy to get        \n\
  \       confusing results with L = 2), and                                     \n\
  \   * 2 when only the size is requested (this avoids to recursively traverse   \n\
  \       directories, which would then be no better than du).                   \n\
  \\n\
  \ --init-db P: three locations are checked for the database directory,         \n\
  \ in this order:                                                               \n\
  \   * at the root of a device (e.g. /mnt/disk)                                 \n\
  \   * at /var/cache/                                                           \n\
  \   * at the XDG cache directory (usually ~/.cache/), created by default       \n\
  \ The first of these allowing reading and writing is selected.                 \n\
  \ The first two are only created on demand, with --init-db P,                  \n\
  \ when P = /var/cache or e.g. P = /mnt/disk respectively.                      "


-- * main

main :: IO ()
main = do
  mapM_ mkTranslitEncoding [stdout, stderr, stdin]

  opt <- customExecParser (prefs $ columns 79) options
  if | not . null $ optInitDB opt -> createDBDirectory $ optInitDB opt
     | optHelp opt -> printHelp
     | otherwise -> do
         seen <- newIORef Set.empty
         bracket newDB closeDB $ \db -> do
           when (_optHID opt > 1) $ resetHID db

           let printEntry ent = do
                 hid <- if optHID opt
                          then sequence $ getHID' db <$> _hash ent
                          else return Nothing
                 putStrLn . showEntry opt hid $ ent

           let list' = S.each (optPaths opt)
                     & S.mapM (mkEntry opt db False seen)
                     & S.catMaybes
                     & S.filter (\ent -> optMinSize opt * 1024 * 1024 <= _size ent &&
                                         optMinCnt opt <= _cnt ent)

           list <- S.toList_ $ if optSortS opt || optSortD opt || optSortCnt opt
                                 then list'
                                 else S.chain printEntry list'

           when (optSortS opt)   $ forM_ (sortOn _size list) printEntry
           when (optSortD opt)   $ forM_ (sortOn _du   list) printEntry
           when (optSortCnt opt) $ forM_ (sortOn (\e -> if isDir e then _cnt e else -1) list) printEntry
           when (optTotal opt) $
             printEntry $ combine ("*total*", fromIntegral directoryMode, True, 0) list


mkTranslitEncoding :: Handle -> IO ()
mkTranslitEncoding h =
  hGetEncoding h >>= mapM_ (\enc ->
    hSetEncoding h =<< mkTextEncoding (takeWhile (/= '/') (show enc) ++ "//TRANSLIT"))


-- * Entry

data Entry = Entry { _path   :: FilePath
                   , _mode   :: FileMode
                   , _sizeOK :: Bool -- size and du are reliable
                   , _size   :: Int
                   , _du     :: Int
                   , _cnt    :: Int
                   , _hash   :: Maybe ByteString
                   } deriving (Show)


mkEntry  :: Options -> DB -> Bool -> IORef (Set (DeviceID, FileID)) -> FilePath -> IO (Maybe Entry)
mkEntry opt db quiet seen path = do
  status' <- try (getSymbolicLinkStatus path) :: IO (Either IOException FileStatus)
  now' <- getPOSIXTime
  case status' of
    Left exception -> do unless quiet $ hPutStrLn stderr $ "error: " ++ show exception
                         return Nothing
    Right status -> do
        seen' <- if optUnique opt
                   then readIORef seen
                   else return Set.empty
        let devino = (deviceID status, fileID status)
        if optUnique opt && Set.member devino seen'
          then return Nothing
          else do writeIORef seen $ Set.insert devino seen'
                  mkEntry' opt db seen path status now'

mkEntry' :: Options -> DB -> IORef (Set (DeviceID, FileID))
         -> FilePath -> FileStatus -> POSIXTime
         -> IO (Maybe Entry)
mkEntry' opt db seen path status now'
  | isRegularFile status = do
      let newent' = return . Just . Entry path mode True size du 1
          newent put key = do
            h' <- try $ sha1sum path :: IO (Either IOException ByteString)
            case h' of
              Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                                   newent' Nothing
              Right h        -> do _ <- put db dev $ mkDBEntry (key, now, size, du, 1, h)
                                   newent' $ Just h
      if optSHA1' opt
        -- DB access is not lazy so a guard is needed somewhere to avoid computing the hash
        -- we can as well avoid the getDB call in this case, as a small optimization
      then do
        let annexHash = if optATrust opt then getAnnexHash path else Nothing
        if isJust annexHash
          then newent' annexHash
          else do
            key <- mkKey opt db status path
            entry_ <- getDB db dev $ fromKey key
            case entry_ of
              Nothing -> newent insertDB key
              Just (_, t, s, _, _, h, p)
                | s /= size                           -> newent updateDB key
                | not $ keyMatches key p              -> newent updateDB key
                | (cl == 1 || cl == 2) && t >= cmtime -> newent' $ Just h
                | cl == 3                             -> newent' $ Just h
                | otherwise                           -> newent updateDB key
      else newent' Nothing
  | isSymbolicLink status = do
      target <- readSymbolicLink path
      if optSLink opt || optALink opt && ".git/annex/objects/" `isInfixOf` target
      then do
         ent <- mkEntry opt db True seen (takeDirectory path </> target)
         return $ flip fmap ent $ \e -> e { _path = path }
      else
        -- we compute hash conditionally as directories containing only symlinks will otherwise
        -- provoke its evaluation; putting instead the condition when storing dir infos into the DB
        -- seems to make the matter worse (requiring then to make the hash field strict...)
        return . Just . Entry path mode True size du 0 $
          if optSHA1' opt then Just $ sha1sumSymlink target
                          else Nothing
  | isDirectory status = do
      key <- mkKey opt db status path
      let newent put = do
            files' <- try (listDirectory path) :: IO (Either IOException [FilePath])
            case files' of
              Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                                   return . Just $ Entry path mode False size du 0 Nothing
              Right files -> do
                entries <- sequence $ mkEntry opt db False seen . (path </>) <$> files :: IO [Maybe Entry]
                let dir = combine (path, mode, True, du) $ catMaybes entries
                when (_sizeOK dir) $
                  -- if the above condition is true, a read error occured somewhere and the info
                  -- can't reliably be stored into the DB
                  put db dev $ mkDBEntry (key, now, _size dir, _du dir, _cnt dir, fromMaybe B.empty (_hash dir))
                return . Just $ dir
      let hcompat h = not (B.null h && optSHA1' opt)
      entry_ <- getDB db dev $ fromKey key
      case entry_ of
        Nothing -> newent insertDB
        Just (_, t, s, d, n, h, p)
          | not $ keyMatches key p              -> newent updateDB
          | cl == 2 && t >= cmtime && hcompat h -> newent'
          | cl == 3 && hcompat h                -> newent'
          | otherwise                           -> newent updateDB
          where newent' = return . Just $ Entry path mode True s d n (if B.null h then Nothing else Just h)
  | otherwise = return . Just . Entry path mode True size du 0 . Just $ B.pack $ replicate 20 0
  where dev    = deviceID status
        mode   = if optUseModes opt then fileMode status else fileMode status .&. complement accessModes
        cmtime = ceiling $ 10^(9::Int) * (if optMtime opt then modificationTimeHiRes else statusChangeTimeHiRes) status
        now    = ceiling $ 10^(9::Int) * now'
        size   = fromIntegral $ fileSize status
        du     = fileBlockSize status * 512 -- TODO: check 512 is always valid
        cl     = optCLevel opt


-- the 1st param gives non-cumulated (own) size and other data for resulting Entry
combine :: (String, FileMode, Bool, Int) -> [Entry] -> Entry
combine (name, mode, ok0, d0) entries = finalize $ foldl' update (ok0, 0, d0, 0, pure dirCtx) entries'
  where entries' = sortOn _path [ e { _path = takeFileName $ _path e } | e <- entries ]
        update (ok, s, d, n, ctx) (Entry p m ok' size du cnt hash) =
          (ok && ok', s + dirEntrySize p size, d+du, n+cnt, SHA1.update <$> ctx <*>
              fmap B.concat (sequence [hash, Just $ pack32 m, Just $ Char8.pack p, Just $ B.singleton 0]))
              -- The mode of a file has no effect on its hash, only on its containing dir's hash;
              -- this is similar to how git does.
              -- We add '\0' (`singleton 0`) to be sure that 2 unequal dirs won't have the same hash;
              -- this assumes '\0' can't be in a file name.
        finalize (ok, s, d, n, ctx) = Entry name mode ok s d n $ SHA1.finalize <$> ctx
        pack32 m = B.pack $ fromIntegral <$> [m, m `shiftR` 8, m `shiftR` 16, m `shiftR` 24]

-- arbitrary way to compute size for directories
-- + we don't use what is reported by `fileSize status` as this is not consistent
--   accross file systems
-- + an empty dir has null size as this is useful to easily spot
-- + the default reports only the sum of the sizes of contained files
dirEntrySize :: FilePath -> Int -> Int
dirEntrySize _ s = s

-- possibe alternative with an option
-- dirEntrySize p s = s + 2 * length p + 64 -- 64 is arbitrary "size overhead" for each entry


isDir :: Entry -> Bool
isDir e = directoryMode == intersectFileModes directoryMode (_mode e)


-- * Key

data Key = Inode DBKey | PathHash DBKey ByteString


highBit, highBit', highBit'', highBit''' :: DBKey
highBit    = 1 `shiftL` 63
highBit'   = 1 `shiftL` 62
highBit''  = 1 `shiftL` 61
highBit''' = 1 `shiftL` 60

mask60 :: DBKey -> DBKey
mask60 = (.&. (highBit''' - 1))

-- the three high bits of the DBKey part of the Key for directories
-- encode whether symbolic links are followed, git-annex links are
-- followed, or only unique files are handled
mkKey :: Options -> DB -> FileStatus -> FilePath -> IO Key
mkKey opt db status path = do
  target' <- getTarget db $ deviceID status :: IO (Maybe FilePath)
  case target' of
    Nothing ->
      let ino = fromIntegral . fileID $ status :: DBKey
      in if mask60 ino == ino
           then return $ Inode $ bits .|. ino
           else error "unexpected inode number"
    Just target -> do
      realpath <- canonicalizePath path
      let hash = SHA1.hash . fromRawString $ makeRelative target realpath
          ws   = fromIntegral <$> B.unpack hash :: [DBKey]
          h64  = mask60 $ sum [w `shiftL` i | (w, i) <- zip ws [0, 8..56]]
      return $ PathHash (bits .|. h64) hash
  where bit    = if optSLink    opt && isDirectory status then highBit    else 0
        bit'   = if optALink    opt && isDirectory status then highBit'   else 0
        bit''  = if optUnique   opt && isDirectory status then highBit''  else 0
        bit''' = if optUseModes opt && isDirectory status then highBit''' else 0
        bits  = bit .|. bit' .|. bit'' .|. bit'''

fromKey :: Key -> DBKey
fromKey (Inode k)      = k
fromKey (PathHash k _) = k

mkDBEntry :: (Key, Int64, Int, Int, Int, ByteString) -> DBEntry
mkDBEntry (Inode k, t, s, d, n, h)      = (k, t, s, d, n, h, B.empty)
mkDBEntry (PathHash k p, t, s, d, n, h) = (k, t, s, d, n, h, p)

keyMatches :: Key -> ByteString -> Bool
keyMatches (Inode _) p | B.null p  = True
                       | otherwise = error "unexpected key"
keyMatches (PathHash _ p) p' | p == p'   = True
                             | otherwise = False


-- this is a more robust alternative to fromString, which is not injective:
-- indeed, e.g. "Mod\232les" and "Mod\56552les" are made into the same ByteString,
-- which is a bug for the purpose of a crypto hash used as a signature
fromRawString :: String -> ByteString
fromRawString s = B.pack . concat $ charToBytes . ord <$> s
  where charToBytes c = fromIntegral <$> [c, c `shiftR` 8, c `shiftR` 16] -- ord (maxBound :: Char) < 2^24


-- * display

showEntry :: Options -> Maybe Int64 -> Entry -> String
showEntry opt hid ent@(Entry path _ _ size du cnt hash) =
  let np    = if optCnt opt then
                if isDir ent then printf "%14s" ("("++ show cnt ++ ")  ") ++ path
                             else "              " ++ path
                else path
      snp   = if optSize opt then formatSize opt size ++ "  " ++ np               else np
      dsnp  = if optDU opt then formatSize opt du ++ "  " ++ snp                  else snp
      hdsnp = if optSHA1 opt -- if hash is still Nothing, there was an I/O error
                then maybe (replicate 40 '*') hexlify hash ++ "  " ++ dsnp        else dsnp
      in      if optHID opt
                then let s = "#" ++ maybe "*" show hid
                     in printf "%5s  " s ++ hdsnp
                else hdsnp

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
dirCtx = SHA1.update SHA1.init $ B.pack [0x2a, 0xc9, 0xd8, 0x3b, 0xc8, 0x7c, 0xe4, 0x86, 0xb2, 0x41,
                                         0xd2, 0x27, 0xb4, 0x06, 0x93, 0x60, 0xc6, 0x2b, 0x52, 0x37]

-- random seed for symlinks
symlinkCtx :: SHA1.Ctx
symlinkCtx = SHA1.update SHA1.init $ B.pack [0x05, 0xfe, 0x0d, 0x17, 0xac, 0x9a, 0x10, 0xbc, 0x7d, 0xb1,
                                             0x73, 0x99, 0xa6, 0xea, 0x92, 0x38, 0xfa, 0xda, 0x0f, 0x16]

sha1sumSymlink :: String -> ByteString
sha1sumSymlink = SHA1.finalize . SHA1.update symlinkCtx . fromRawString

sha1sum :: FilePath -> IO ByteString
sha1sum = fmap SHA1.hashlazy . BL.readFile

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

getAnnexHash :: String -> Maybe ByteString
getAnnexHash path =
  let parts = splitOn "-" $ takeBaseName path
  in case parts of
     [backend, s, "", hex]
       | backend `elem` ["SHA1", "SHA1E"] && head s == 's' && length hex == 40 -> unhexlify hex
       | otherwise -> Nothing
     _ -> Nothing

-- use #0 and #-1 resp. for empty files and directories
getHID' :: DB -> ByteString -> IO Int64
getHID' db hash
  | hash == SHA1.finalize SHA1.init = return 0
  | hash == SHA1.finalize dirCtx    = return (-1)
  | otherwise                       = getHID db hash
