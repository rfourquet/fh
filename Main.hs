module Main where

import           Control.Exception     (IOException, bracket, try)
import           Control.Monad         (forM, forM_, unless, when)
import qualified Crypto.Hash.SHA1      as SHA1
import           Data.Bits             (shiftL, shiftR, (.&.), (.|.))
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy  as BL
import           Data.Char             (ord)
import           Data.Int              (Int64)
import           Data.IORef
import           Data.List             (find, foldl', sort, sortOn)
import           Data.Maybe            (catMaybes, fromJust, fromMaybe, isJust)
import           Data.Set              (Set)
import qualified Data.Set              as Set
import           Data.Time.Clock.POSIX
import           Data.Word             (Word32)
import           Numeric               (showHex)
import           Options.Applicative   (Parser, ParserInfo, ReadM, argument, auto, execParser,
                                        flag', footer, fullDesc, help, helper, info, long, many,
                                        metavar, option, progDesc, readerError, short, str, switch,
                                        value)
import           System.Directory      (canonicalizePath, listDirectory)
import           System.FilePath       (makeRelative, takeDirectory, takeFileName, (<.>), (</>))
import           System.IO             (Handle, hGetEncoding, hPutStrLn, hSetEncoding,
                                        mkTextEncoding, stderr, stdin, stdout)
import           System.Posix.Files    (FileStatus, deviceID, directoryMode, fileID, fileMode,
                                        fileSize, getSymbolicLinkStatus, intersectFileModes,
                                        isDirectory, isRegularFile, isSymbolicLink,
                                        modificationTimeHiRes, readSymbolicLink,
                                        statusChangeTimeHiRes)
import           Text.Printf           (printf)

import           DB
import qualified Mnt
import           Stat                  (fileBlockSize)


-- * Options

data Options = Options { _optSHA1   :: Bool
                       , _optHID    :: Int
                       , _optSize   :: Bool
                       , optDU      :: Bool
                       , optCnt     :: Bool
                       , optTotal   :: Bool

                       , _optCLevel :: CacheLevel
                       , optMtime   :: Bool
                       , optSLink   :: Bool
                       , optUnique  :: Bool
                       , optSI      :: Bool
                       , _optSort   :: Bool
                       , optSortCnt :: Bool
                       , optNoGit   :: Bool
                       , _optPaths  :: [FilePath]
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

optSort :: Options -> Bool
optSort opt = _optSort opt && not (optSortCnt opt)

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
                <$> switch (long "sha1" <> short 'x' <>
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
                <*> switch (long "unique" <> short 'u' <>
                            help "discard files which have already been accounted for")
                <*> switch (long "si" <> short 't' <>
                            help "use powers of 1000 instead of 1024 for sizes")
                -- TODO make the 2 sorting options mutually exclusive
                <*> switch (long "sort" <> short 'S' <>
                            help "sort output, according to size")
                <*> switch (long "sort-count" <> short 'N' <>
                            help "sort output, according to count")
                <*> switch (long "ignore-git" <> short 'G' <>
                            help "ignore \".git\" filenames passed on the command line")
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
                    \ or when the unique option is active (it's otherwise easy to get confusing results with   \
                    \ l = 2), and 2 when only the size is requested (this avoids to recursively traverse       \
                    \ directories, which would then be no better than du)."

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
  seen <- newIORef Set.empty
  bracket newDB closeDB $ \db -> do
    when (_optHID opt > 1) $ resetHID db

    let printEntry ent = do
          hid <- if optHID opt
                   then sequence $ getHID db <$> _hash ent
                   else return Nothing
          putStrLn . showEntry opt hid $ ent

    mps <- filter (isJust . Mnt.uuid) <$> Mnt.points

    list_ <- forM (mkEntry opt db seen mps <$> optPaths opt) $ \entIO_ -> do
      ent_ <- entIO_
      case ent_ of
        Nothing -> return Nothing
        Just ent -> do
          unless (optSort opt || optSortCnt opt) (printEntry ent)
          return ent_

    let list = catMaybes list_
    when (optSort opt)    $ forM_ (sortOn _size list) printEntry
    when (optSortCnt opt) $ forM_ (sortOn (\e -> if isDir e then _cnt e else -1) list) printEntry
    when (optTotal opt) $
      printEntry $ combine ("*total*", fromIntegral directoryMode, True, 0, 0)
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
                   , _cnt    :: Int
                   , _hash   :: Maybe ByteString
                   } deriving (Show)


mkEntry :: Options -> DB -> IORef (Set (Int64, Int64)) -> [Mnt.Point] -> FilePath -> IO (Maybe Entry)
mkEntry opt db seen mps path = do
  status' <- try (getSymbolicLinkStatus path) :: IO (Either IOException FileStatus)
  now' <- getPOSIXTime
  case status' of
    Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                         return Nothing
    Right status -> do
        key <- maybe (return NoKey) (mkKey status path (optSLink opt) (optUnique opt)) mp -- key won't be used when mp == Nothing
        seen' <- if optUnique opt
                   then readIORef seen
                   else return Set.empty
        let devino = (dev, fromIntegral . fileID $ status)
        if optUnique opt && Set.member devino seen'
          then return Nothing
          else do writeIORef seen $ Set.insert devino seen'
                  mkEntry' opt db seen mps path status key dbpath now'
      where dev    = fromIntegral $ deviceID status
            mp     = find ((== dev) . Mnt.devid) mps
            uuid   = mp >>= Mnt.uuid :: Maybe String
            dbpath = (\x -> "/var/cache/fh" </> x <.> "db") <$> uuid :: Maybe FilePath

mkEntry' :: Options -> DB -> IORef (Set (Int64, Int64)) -> [Mnt.Point] -> FilePath
         -> FileStatus -> Key -> Maybe FilePath -> POSIXTime
         -> IO (Maybe Entry)
mkEntry' opt db seen mps path status key dbpath now'
  | isRegularFile status = do
      let newent' = return . Just . Entry path mode True size du 1
          newent put = do
            h' <- try $ sha1sum path :: IO (Either IOException ByteString)
            case h' of
              Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                                   newent' Nothing
              Right h        -> do _ <- put db dbpath $ mkDBEntry (key, now, size, du, 1, h)
                                   newent' $ Just h
      if optSHA1' opt
        -- DB access is not lazy so a guard is needed somewhere to avoid computing the hash
        -- we can as well avoid the getDB call in this case, as a small optimization
        then do
          entry_ <- getDB db dbpath $ fromKey key
          case entry_ of
            Nothing -> newent insertDB
            Just (_, t, s, _, _, h, p)
              | s /= size                           -> newent updateDB
              | not $ keyMatches key p              -> newent updateDB
              | (cl == 1 || cl == 2) && t >= cmtime -> newent' $ Just h
              | cl == 3                             -> newent' $ Just h
              | otherwise                           -> newent updateDB
        else newent' Nothing
  | isSymbolicLink status =
      if optSLink opt
      then do
         ent <- mkEntry opt db seen mps =<< fmap (takeDirectory path </>) (readSymbolicLink path)
         return $ flip fmap ent $ \e -> e { _path = path }
      else
        -- we compute hash conditionally as directories containing only symlinks will otherwise
        -- provoke its evaluation; putting instead the condition when storing dir infos into the DB
        -- seems to make the matter worse (requiring then to make the hash field strict...)
        Just . Entry path mode True size du 1 <$>
          if optSHA1' opt then Just <$> sha1sumSymlink path
                          else return Nothing
  | isDirectory status = do
      let newent put = do
            files' <- try (listDirectory path) :: IO (Either IOException [FilePath])
            case files' of
              Left exception -> do hPutStrLn stderr $ "error: " ++ show exception
                                   return . Just $ Entry path mode False size du 0 Nothing
              Right files -> do
                entries <- sequence $ mkEntry opt db seen mps . (path </>) <$> sort files :: IO [Maybe Entry]
                let dir = combine (path, mode, True, size, du) $ catMaybes entries
                when (_sizeOK dir) $
                  -- if the above condition is true, a read error occured somewhere and the info
                  -- can't reliably be stored into the DB
                  put db dbpath $ mkDBEntry (key, now, _size dir, _du dir, _cnt dir, fromMaybe B.empty (_hash dir))
                return . Just $ dir
      let hcompat h = not (B.null h && optSHA1' opt)
      entry_ <- getDB db dbpath $ fromKey key
      case entry_ of
        Nothing -> newent insertDB
        Just (_, t, s, d, n, h, p)
          | not $ keyMatches key p              -> newent updateDB
          | cl == 2 && t >= cmtime && hcompat h -> newent'
          | cl == 3 && hcompat h                -> newent'
          | otherwise                           -> newent updateDB
          where newent' = return . Just $ Entry path mode True s d n (if B.null h then Nothing else Just h)
  | otherwise = return . Just . Entry path mode True size du 0 . Just $ B.pack $ replicate 20 0
  where mode   = fromIntegral $ fileMode status
        cmtime = ceiling $ 10^(9::Int) * (if optMtime opt then modificationTimeHiRes else statusChangeTimeHiRes) status
        now    = ceiling $ 10^(9::Int) * now'
        size   = fromIntegral $ fileSize status
        du     = fileBlockSize status * 512 -- TODO: check 512 is always valid
        cl     = optCLevel opt


-- the 1st param gives non-cumulated (own) size and other data for resulting Entry
combine :: (String, Word32, Bool, Int, Int) -> [Entry] -> Entry
combine (name, mode, ok0, s0, d0) entries = finalize $ foldl' update (ok0, s0, d0, 0, pure dirCtx) entries
  where update (ok, s, d, n, ctx) (Entry p m ok' size du cnt hash) =
          (ok && ok', s+size, d+du, n+cnt, SHA1.update <$> ctx <*>
              fmap B.concat (sequence [hash, Just $ pack32 m, Just $ Char8.pack $ takeFileName p, Just $ B.singleton 0]))
              -- The mode of a file has no effect on its hash, only on its containing dir's hash;
              -- this is similar to how git does.
              -- We add '\0' (`singleton 0`) to be sure that 2 unequal dirs won't have the same hash;
              -- this assumes '\0' can't be in a file name.
        finalize (ok, s, d, n, ctx) = Entry name mode ok s d n $ SHA1.finalize <$> ctx
        pack32 m = B.pack $ fromIntegral <$> [m, m `shiftR` 8, m `shiftR` 16, m `shiftR` 24]


isDir :: Entry -> Bool
isDir e = directoryMode == intersectFileModes directoryMode (fromIntegral $ _mode e)

-- * Key

data Key = NoKey | Inode Int64 | PathHash Int64 ByteString

highBit :: Int64
highBit = 1 `shiftL` 63
highBit' :: Int64
highBit' = 1 `shiftL` 62

mask62 :: Int64 -> Int64
mask62 = (.&. (highBit' - 1))

-- the high bit of the Int64 part of the Key encodes whether
-- symbolic links are followed
mkKey :: FileStatus -> FilePath -> Bool -> Bool -> Mnt.Point -> IO Key
mkKey status path follow unique mp =
  if Mnt.fstype mp `elem` ["ext2", "ext3", "ext4"]
  then let ino = fromIntegral . fileID $ status :: Int64
       in if mask62 ino == ino
          then return $ Inode $ bits .|. ino
          else error "unexpected negative inode number"
  else do
    realpath <- canonicalizePath path
    let hash = SHA1.hash . fromRawString $ makeRelative (Mnt.target mp) realpath
        ws   = fromIntegral <$> B.unpack hash :: [Int64]
        h64  = mask62 $ sum [w `shiftL` i | (w, i) <- zip ws [0, 8..56]]
    return $ PathHash (bits .|. h64) hash
  where bit1 = if follow && isDirectory status then highBit else 0
        bit2 = if unique && isDirectory status then highBit' else 0
        bits = bit1 .|. bit2

fromKey :: Key -> Int64
fromKey (Inode k)      = k
fromKey (PathHash k _) = k
fromKey NoKey          = error "invalid key"

mkDBEntry :: (Key, Int64, Int, Int, Int, ByteString) -> DBEntry
mkDBEntry (Inode k, t, s, d, n, h)      = (k, t, s, d, n, h, B.empty)
mkDBEntry (PathHash k p, t, s, d, n, h) = (k, t, s, d, n, h, p)
mkDBEntry _                             = error "invalid key"

keyMatches :: Key -> ByteString -> Bool
keyMatches (Inode _) p | B.null p  = True
                       | otherwise = error "unexpected key"
keyMatches (PathHash _ p) p' | p == p'   = True
                             | otherwise = False
keyMatches NoKey _ = error "invalid key"

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

sha1sumSymlink :: FilePath -> IO ByteString
sha1sumSymlink path = SHA1.finalize . SHA1.update symlinkCtx . fromRawString <$> readSymbolicLink path

sha1sum :: FilePath -> IO ByteString
sha1sum = fmap SHA1.hashlazy . BL.readFile

hexlify :: ByteString -> String
hexlify bstr = let words8 = B.unpack bstr
                   showHexPad x = if x >= 16 then showHex x else showChar '0' . showHex x
                   hex = map showHexPad words8
               in foldr ($) "" hex
