{-# LANGUAGE MultiWayIf, OverloadedStrings #-}

module Fh where

import           Control.Applicative         ((<|>))
import           Control.Exception           (IOException, bracket, try)
import           Control.Monad               (forM_, join, unless, when, (<=<))
import           Control.Monad.Trans.Class   (lift)
import qualified Crypto.Hash.SHA1            as SHA1
import           Data.Bits                   (Bits, bit, complement, finiteBitSize, setBit, shiftL,
                                              shiftR, (.&.), (.|.))
import           Data.ByteString             (ByteString)
import qualified Data.ByteString             as B
import qualified Data.ByteString.Char8       as BC
import qualified Data.ByteString.UTF8        as B8
import           Data.Char                   (ord)
import           Data.Function               ((&))
import           Data.Int                    (Int64)
import           Data.IORef
import           Data.List                   (find, foldl', sort, sortOn)
import           Data.Maybe                  (catMaybes, fromJust, fromMaybe, isJust, isNothing)
import           Data.Set                    (Set)
import qualified Data.Set                    as Set
import           Data.Time.Clock.POSIX
import           Options.Applicative         (Parser, ParserInfo, ReadM, argument, auto, columns,
                                              execParserPure, flag', fullDesc, handleParseResult,
                                              help, helper, info, long, many, metavar, option,
                                              optional, prefs, progDesc, readerError, short, str,
                                              strOption, switch, value)
import qualified Streaming.Prelude           as S
import           System.Directory            (canonicalizePath)
import           System.Environment          (getArgs)
import           System.FilePath             (makeRelative)
import           System.IO                   (Handle, hGetEncoding, hPutStrLn, hSetEncoding,
                                              mkTextEncoding, stderr, stdin, stdout)
import           System.Posix.ByteString     (RawFilePath)
import qualified System.Posix.Env.ByteString
import           System.Posix.Files          (FileStatus, accessModes, deviceID, directoryMode,
                                              fileID, fileMode, fileSize, intersectFileModes,
                                              isDirectory, isRegularFile, isSymbolicLink,
                                              modificationTimeHiRes, regularFileMode,
                                              statusChangeTimeHiRes)
import           System.Posix.Types          (DeviceID, FileID, FileMode)
import           Text.Printf                 (printf)

import           DB
import           Hashing
import           RawPath                     ((</>))
import qualified RawPath                     as R
import           Stat                        (fileBlockSize)


-- * Options

data Options = Options { optHelp     :: Bool

                       , _optSHA1    :: Bool
                       , _optHID     :: Int
                       , _optSize    :: Bool
                       , optDU       :: Bool
                       , optCnt      :: Bool
                       , optTotal    :: Bool

                       , optDepth    :: Int
                       , _optCLevel  :: CacheLevel
                       , optMtime    :: Bool
                       , optSLink    :: Bool
                       , optALink    :: Bool
                       , optUnique   :: Bool
                       , optATrust   :: Bool
                       , optAPretend :: Bool
                       , optUseModes :: Bool
                       , optSI       :: Bool
                       , optMinSize  :: Int
                       , optMinCnt   :: Int
                       , _optSortS   :: Bool
                       , _optSortD   :: Bool
                       , optSortCnt  :: Bool
                       , optNoGit    :: Bool
                       , optOptimS   :: Bool
                       , optInitDB   :: FilePath
                       , optInput    :: Maybe FilePath  -- file containing paths to report
                       , optPaths    :: [RawFilePath]
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

                <*> option auto (long "depth" <> short 'R' <> value 0 <> metavar "INT" <>
                                 help "report entries recursively up to depth INT")
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
                <*> switch (long "pretend-annex" <> short 'P' <>
                            help "pretend original files replace git-annex symlinks")
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
                <*> switch (long "optimize-space" <> short 'O' <>
                            help "don't store in DB fast to compute entries")
                <*> strOption (long "init-db" <> metavar "PATH" <> value "" <>
                               help "create a DB directory at PATH and exit")
                <*> optional (strOption (long "files-from" <> short 'I' <> metavar "FILE" <>
                                         help "a file containing paths to work on (\"-\" for stdin)"))
                <*> many (B8.fromString <$> argument str (metavar "PATHS..." <> help "files or directories (default: \".\")"))


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
  \   * 1 when hashes are requested (should be reliable in most cases),          \n\
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

-- ** command arguments pre-parsing

data Arg = Switch | Option | DoubleDash | File
           deriving (Show, Eq)

shortOptions :: ByteString
shortOptions = "RlzkI"

longOptions :: [ByteString]
longOptions = ["depth", "cache-level", "minsize", "mincount", "init-db", "files-from"]

parseArg :: ByteString -> Arg
parseArg arg
  | B.null arg || BC.head arg /= '-' = File
  | B.null arg'                      = File       -- "-"
  | BC.head arg' /= '-'              =            -- single-dashed option group
      if | B.null argO              -> Switch
         | B.null argO'             -> Option
         | otherwise                -> Switch     -- option argument is "glued"
  | BC.null arg''                    = DoubleDash
  | arg'' `elem` longOptions         = Option
  | otherwise                        = Switch     -- switch or glued option argument
    where arg'  = B.tail arg
          arg'' = B.tail arg'
          argO  = B.dropWhile (not . (`B.elem` shortOptions)) arg'
          argO' = B.tail argO

preParseArgs :: ([String], [ByteString]) -> ([String], [ByteString])
preParseArgs = filterPath Switch
  where
    filterPath l (s:ss, b:bs) = if isPath then (ss', b:bs') else (s:ss', bs')
      where (ss', bs') = filterPath n (ss, bs)
            (isPath, n) = if
              | l == DoubleDash -> (True,  l)
              | l == Option     -> (False, Switch)
              | c == File       -> (True,  c)
              | otherwise       -> (False, c)
            c = parseArg b
    filterPath _ _ = ([], [])


-- * main

type Seen = IORef (Set (DeviceID, FileID))

type FhConsumer = Options -> DB -> S.Stream (S.Of Entry) IO () -> IO ()

fh :: FhConsumer -> [String] -> [RawFilePath] -> IO ()
fh consumer args paths = do
  -- paths coming from args will be UTF8-encoded
  opt' <- handleParseResult $ execParserPure (prefs $ columns 79) options args
  let opt = opt' { optPaths = optPaths opt' ++ paths }
  if | not . null $ optInitDB opt -> createDBDirectory $ optInitDB opt
     | optHelp opt -> printHelp
     | otherwise -> do
         seen <- newIORef Set.empty :: IO Seen
         -- `now` is computed only once so that it's possible to easily detect whether an entry has
         -- already been computed in this run, in which case the DB value can be used regardless of
         -- the cache level (this can save significant time, e.g. 2x or 3x speed-up).
         -- The small drawback is if a file is updated after the program started but before being
         -- seen by it (a re-computation could be triggered unnecessarily in a subsequent run).
         -- The `seen` set could also be used, but it's more heavy to maintain when the --unique
         -- option is not active.
         now <- ceiling . (10^(9::Int) *) <$> getPOSIXTime :: IO DBTime

         bracket newDB closeDB $ \db -> do
           when (_optHID opt > 1) $ resetHID db

           optPaths' opt & S.mapM (uncurry $ mkEntry' opt db now seen Nothing)
                         & S.catMaybes
                         & S.filter (\ent -> optMinSize opt * 1024 * 1024 <= _size ent &&
                                             optMinCnt opt <= _cnt ent)
                         & consumer opt db

-- like fh, but returns the result as a list (no consumer fallback)
fh' :: [String] -> [RawFilePath] -> IO [Entry]
fh' args paths = do
  list <- newIORef []
  let writeList _ _ entries = writeIORef list =<< S.toList_ entries
  fh writeList args paths
  readIORef list

fhCLI :: IO ()
fhCLI = do mapM_ mkTranslitEncoding [stdout, stderr, stdin]
           -- optparse-applicative (OA) gets [String] as input, but we want paths as RawFilePath;
           -- it's not clear how to convert a FilePath (output of OA) into a ByteString: it would
           -- require to get the correct encoding (i.e. the one used by GHC to convert CString to
           -- String), and deal with invalid characters (GHC doesn't seem to loose information, i.e.
           -- the FilePath can be used to e.g. read a file, but it's not clear how to convert back
           -- this invalid Char back to a raw string, so that the deduced RawFilePath can be used to
           -- access the file.
           -- Moreover, OA is slow to parse long command lines (composed of many path arguments).
           -- So we bypass it by extracting the paths directly as RawFilePath
           argsS <- getArgs
           argsB <- System.Posix.Env.ByteString.getArgs
           uncurry (fh fhPrint) $ preParseArgs (argsS, argsB)

fhPrint :: FhConsumer
fhPrint opt db entries = do
  let printEntry = putStrLn <=< showEntry opt db

  list <- S.toList_ $ if optSortS opt || optSortD opt || optSortCnt opt
                        then entries
                        else S.chain printEntry entries

  when (optSortS opt)   $ forM_ (sortOn _size list) printEntry
  when (optSortD opt)   $ forM_ (sortOn _du   list) printEntry
  when (optSortCnt opt) $ forM_ (sortOn (\e -> if isDir e then _cnt e else -1) list) printEntry
  when (optTotal opt) $
    printEntry $ combine ("*total*", fromIntegral directoryMode, True, 0) list


-- recursively yield paths within directories up to a fixed depth
optPaths' :: Options -> S.Stream (S.Of (RawFilePath, FileStatus)) IO ()
optPaths' opt = do
    input <- flip (maybe (return [])) (optInput opt) $ \file ->
               BC.lines <$> lift (if file == "-" then B.getContents else B.readFile file)
    let paths = if isNothing (optInput opt) && null (optPaths opt) then [BC.singleton '.'] else optPaths opt
    S.for (S.each $ input ++ paths) $ recStatus $ optDepth opt
  where
    recStatus :: Int -> RawFilePath -> S.Stream (S.Of (RawFilePath, FileStatus)) IO ()
    recStatus depth path =
        -- filtering ".git" here rather than at the toplevel pipeline (in main)
        -- saves work (the recursive content of ".git" has to be pruned)
        unless (optNoGit opt && R.takeFileName path == ".git") $ do  -- TODO: encoding?
          status' <- lift $ getStatus path False
          S.for (S.each status') $ \status -> do
            when (depth > 0 && isDirectory status) $ do
              files' <- lift (try (R.listDirectory path) :: IO (Either IOException [RawFilePath]))
              -- we ignore exceptions here, they may reported later within mkEntry
              S.for (S.each files') $ \files ->
                S.for (S.each (sort files)) $ recStatus (depth-1) . R.combine' path
            S.yield (path, status)


mkTranslitEncoding :: Handle -> IO ()
mkTranslitEncoding h =
  hGetEncoding h >>= mapM_ (\enc ->
    hSetEncoding h =<< mkTextEncoding (takeWhile (/= '/') (show enc) ++ "//TRANSLIT"))


-- * Entry

data Entry = Entry { _path   :: RawFilePath
                   , _mode   :: FileMode
                   , _sizeOK :: Bool -- size and du are reliable
                   , _size   :: Int
                   , _du     :: Int
                   , _cnt    :: Int
                   , _hash   :: Maybe ByteString
                   } deriving (Show)


getStatus :: RawFilePath -> Bool -> IO (Maybe FileStatus)
getStatus path quiet = do
  status' <- try (R.getSymbolicLinkStatus path) :: IO (Either IOException FileStatus)
  case status' of
    Left exception -> do unless quiet $ hPutStrLn stderr $ "error: " ++ show exception
                         return Nothing
    Right status -> return $ Just status

mkEntry :: Options -> DB -> DBTime -> Seen -> Maybe ByteString -> RawFilePath -> IO (Maybe Entry)
mkEntry opt db now seen mHash path =
          fmap join $ mapM (mkEntry' opt db now seen mHash path) =<< getStatus path (isJust mHash)

updateSeen :: Options -> Seen -> FileStatus -> Maybe FileID -> IO Bool
updateSeen opt seen status key =
  if optUnique opt
    then do
      seen' <- readIORef seen
      let devino = (deviceID status, fromMaybe (fileID status) key)
      if Set.member devino seen'
        then return False
        else do writeIORef seen $ Set.insert devino seen'
                return True
    else return True

mkEntry' :: Options -> DB -> DBTime -> Seen -> Maybe ByteString ->
            RawFilePath -> FileStatus -> IO (Maybe Entry)
mkEntry' opt db now seen mHash path status = do
  continue <- updateSeen opt seen status Nothing
  if continue then mkEntry'' opt db now seen mHash path status else return Nothing

mkEntry'' :: Options -> DB -> DBTime -> Seen
          -> Maybe ByteString -> RawFilePath -> FileStatus
          -> IO (Maybe Entry)
mkEntry'' opt db now seen mHash path status
  | isRegularFile status = do
      let newent' = return . Just . Entry path mode True size du 1
          newent exists key = do
            h' <- try $ sha1sum path :: IO (Either IOException ByteString)
            case h' of
              Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                                   newent' Nothing
              Right h        -> do
                if optOptimS opt && du <= 4096
                  -- quite arbitrary threshold: smaller values don't change performance much: there
                  -- is always significant overhead for having to read file content at all
                  then when exists $ deleteDB db dev $ fromKey key
                  else let put = if exists then updateDB else insertDB
                       in  put db dev $ mkDBEntry (key, now, size, du, 1, h)
                newent' $ Just h
      if optSHA1' opt
        -- DB access is not lazy so a guard is needed somewhere to avoid computing the hash
        -- we can as well avoid the getDB call in this case, as a small optimization
      then do
        let annexSzHash = optATrust opt
                        |>> mHash <|> snd <$> getAnnexSizeAndHash False path
        if isJust annexSzHash
          then newent' annexSzHash
          else do
            key <- mkKey opt db status path
            entry_ <- getDB db dev $ fromKey key
            case entry_ of
              Nothing -> newent False key
              Just (_, t, s, _, _, h, p)
                | s /= size                            ||
                  not (keyMatches key p)               -> newent True key
                | t == now                             ||
                  (cl == 1 || cl == 2) && t >= cmtime  ||
                  cl == 3                              -> newent' $ Just h
                | otherwise                            -> newent True key
      else newent' Nothing
  | isSymbolicLink status = do
      target <- R.readSymbolicLink path
      let annexSzHash = optAPretend opt || optALink opt
                      |>> getAnnexSizeAndHash True target
      if | optAPretend opt && isJust annexSzHash -> do
             let Just (s, h) = annexSzHash
                 key = toIntegral h `setBit` (finiteBitSize (0::FileID) - 1)
                     -- we make sure the high bit is set to reduce risk of collisions
             continue <- updateSeen opt seen status $ Just key
             return $ continue
                      |>> (Just . Entry path regularFileMode True s (guessDU s) 1 $
                                    optSHA1' opt |>> Just h)
         | optSLink opt || optALink opt && isJust annexSzHash -> do
             ent <- mkEntry opt db now seen (snd <$> annexSzHash) (R.takeDirectory path </> target)
             return $ flip fmap ent $ \e -> e { _path = path }
         | otherwise ->
             -- we compute hash conditionally as directories containing only symlinks will otherwise
             -- provoke its evaluation; putting instead the condition when storing dir infos into the DB
             -- seems to make the matter worse (requiring then to make the hash field strict...)
             return . Just . Entry path mode True size du 0 $
               optSHA1' opt |>> Just (sha1sumSymlink target)
  | isDirectory status = do
      key <- mkKey opt db status path
      let newent exists = do
            files' <- try (R.listDirectory path) :: IO (Either IOException [RawFilePath])
            case files' of
              Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                                   return . Just $ Entry path mode False size du 0 Nothing
              Right files -> do
                entries <- sequence $ mkEntry opt db now seen Nothing . R.combine' path <$> files :: IO [Maybe Entry]
                let dir = combine (path, mode, True, du) $ catMaybes entries
                if _sizeOK dir &&
                   -- if the above condition is true, a read error occured somewhere and the info
                   -- can't reliably be stored into the DB
                   not (optOptimS opt && (null files || null (tail files))) -- don't store dirs of length <= 1
                  then
                    let put = if exists then updateDB else insertDB
                    in  put db dev $ mkDBEntry (key, now, _size dir, _du dir, _cnt dir, fromMaybe B.empty (_hash dir))
                  else when exists $ deleteDB db dev $ fromKey key
                return . Just $ dir
      entry_ <- getDB db dev $ fromKey key
      case entry_ of
        Nothing -> newent False
        Just (_, t, s, d, n, h, p)
          | B.null h && optSHA1' opt || -- hash requested but not available
            not (keyMatches key p)   -> newent True
          | t == now                 ||
            cl == 2 && t >= cmtime   ||
            cl == 3                  -> newent'
          | otherwise                -> newent True
          where newent' = return . Just $ Entry path mode True s d n (if B.null h then Nothing else Just h)
  | otherwise = return . Just . Entry path mode True size du 0 . Just $ B.pack $ replicate 20 0
  where dev    = deviceID status
        mode   = if optUseModes opt then fileMode status else fileMode status .&. complement accessModes
        cmtime = ceiling $ 10^(9::Int) * (if optMtime opt then modificationTimeHiRes else statusChangeTimeHiRes) status
        size   = fromIntegral $ fileSize status
        du     = fileBlockSize status * 512 -- TODO: check 512 is always valid
        cl     = optCLevel opt
        guessDU s = 4096 * (1 + div (s-1) 4096)

-- the 1st param gives non-cumulated (own) size and other data for resulting Entry
combine :: (RawFilePath, FileMode, Bool, Int) -> [Entry] -> Entry
combine (name, mode, ok0, d0) entries = finalize $ foldl' update (ok0, 0, d0, 0, pure dirCtx) entries'
  where entries' = sortOn _path [ e { _path = R.takeFileName $ _path e } | e <- entries ]
        update (ok, s, d, n, ctx) (Entry p m ok' size du cnt hash) =
          (ok && ok', s + dirEntrySize p size, d+du, n+cnt, SHA1.update <$> ctx <*>
              fmap B.concat (sequence [hash, Just $ pack32 m, Just p, Just $ B.singleton 0]))
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
dirEntrySize :: RawFilePath -> Int -> Int
dirEntrySize _ s = s

-- possibe alternative with an option
-- dirEntrySize p s = s + 2 * length p + 64 -- 64 is arbitrary "size overhead" for each entry


isDir :: Entry -> Bool
isDir e = directoryMode == intersectFileModes directoryMode (_mode e)


-- * Key

data Key = Inode DBKey | PathHash DBKey ByteString

-- the five high bits of the DBKey part of the Key for directories
-- encode whether some options are active
mkKey :: Options -> DB -> FileStatus -> RawFilePath -> IO Key
mkKey opt db status path = do
  target' <- getTarget db $ deviceID status :: IO (Maybe FilePath)
  case target' of
    Nothing ->
      let ino = fromIntegral . fileID $ status :: DBKey
      in if mask ino == ino
           then return $ Inode $ bits .|. ino
           else error "unexpected inode number"
    Just target -> do
      let path' = B8.toString path
      realpath <- canonicalizePath path'
      let hash = SHA1.hash . fromRawString $ makeRelative target realpath
          ws   = fromIntegral <$> B.unpack hash :: [DBKey]
          h64  = mask $ sum [w `shiftL` i | (w, i) <- zip ws [0, 8..56]]
      return $ PathHash (bits .|. h64) hash
  where bits = if optSLink    opt && isDirectory status then bit 63 else 0
           .|. if optALink    opt && isDirectory status then bit 62 else 0
           .|. if optAPretend opt && isDirectory status then bit 61 else 0
           .|. if optUnique   opt && isDirectory status then bit 60 else 0
           .|. if optUseModes opt && isDirectory status then bit 59 else 0
        mask :: DBKey -> DBKey
        mask = (.&. (bit 59 - 1))

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

showEntry :: Options -> DB -> Entry -> IO String
showEntry opt db ent = showEntry' opt ent <$>
  if optHID opt
    then sequence $ getHID' db <$> _hash ent
    else return Nothing

-- TODO: decode via UTF8 only when interactive (stdout is a tty)
-- TODO: use system encoding instead of hardcoded UTF8
showEntry' :: Options -> Entry -> Maybe Int64 -> String
showEntry' opt ent@(Entry path' _ _ size du cnt hash) hid = mconcat
  [ if optHID opt then printf "%5s  " $ "#" ++ maybe "*" show hid else ""
  , if optSHA1 opt -- if hash is still Nothing, there was an I/O error
      then maybe (replicate 40 '*') hexlify hash ++ "  "          else ""
  , if optDU opt then formatSize opt du ++ "  "                   else ""
  , if optSize opt then formatSize opt size ++ "  "               else ""
  , if optCnt opt then
      if isDir ent then printf "%14s" ("("++ show cnt ++ ")  ")
                   else "              "
                                                                  else ""
  , B8.toString path'
  ]

formatSize :: Options -> Int -> String
formatSize opt sI =
  let u0:units = (if optSI opt then (++"B") else id) <$>
                 [" ", "k", "M", "G", "T", "P", "E", "Z", "Y"]
      base = if optSI opt then 1000.0 else 1024.0
      (s', u) = fromJust $ find (\(s, _) -> s < 1000.0) $
                  scanl (\(s, _) f' ->  (s / base, f')) (fromIntegral sI :: Double, u0) units
  in printf (if head u == ' ' then "%3.f  %s" else "%5.1f%s") s' u


-- * misc.

-- use #0 and #-1 resp. for empty files and directories
getHID' :: DB -> ByteString -> IO Int64
getHID' db hash
  | hash == SHA1.finalize SHA1.init = return 0
  | hash == SHA1.finalize dirCtx    = return (-1)
  | otherwise                       = getHID db hash


toIntegral :: (Integral a, Bits a) => ByteString -> a
toIntegral = fst . B.foldl' f (0, 0)
  where f (s, i) w8 = (s + fromIntegral w8 `shiftL` i, i+8)


(|>>) :: Bool -> Maybe a -> Maybe a
b |>> m = if b then m else Nothing

infixl 1 |>>
