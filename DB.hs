{-# LANGUAGE OverloadedStrings #-}

module DB (DB, DBEntry, DBKey, DBTime, closeDB, createDBDirectory, getDB,
           getHID, getTarget, insertDB, newDB, deleteDB, resetHID, updateDB) where

import Control.Concurrent.Thread.Delay (delay)
import           Control.Monad      (join, mapM_, (>=>), when)
import qualified Data.ByteString    as BS
import           Data.Int           (Int8, Int64)
import           Data.IORef
import           Data.List          (find)
import           Data.Maybe         (fromMaybe, isJust)
import           Data.String        (fromString)
import           Database.SQLite3   (ColumnType (..), Database, SQLData (..), Statement,
                                     StepResult (..), bind, bindBlob, bindInt64, close, exec,
                                     finalize, open, prepare, reset, step, typedColumns)
import           System.Directory   (XdgDirectory (XdgCache), canonicalizePath,
                                     createDirectoryIfMissing, doesDirectoryExist, getPermissions,
                                     getXdgDirectory, readable, writable)
import           System.FilePath    (dropTrailingPathSeparator, (<.>), (</>))
import           System.IO          (hPutStrLn, stderr)
import           System.Posix.Types (DeviceID)
import System.Random

import qualified Mnt


fhID :: String
fhID = "XSfaWmMBsldaiatb9rjmMwKers"

version :: Int
version = 0


-- * public API

type DBKey = Int64
type DBTime = Int64
type DBEntry = (DBKey, DBTime, Int, Int, Int, BS.ByteString, BS.ByteString)

data DB = DB [Mnt.Point] (IORef (Maybe DB')) (IORef (Maybe IDMap))


newDB :: IO DB
newDB =
  DB <$> (filter (isJust . Mnt.uuid) <$> Mnt.points)
     <*> newIORef Nothing
     <*> newIORef Nothing

closeDB :: DB -> IO ()
closeDB (DB _ db' dbm) = do
   readIORef db' >>= mapM_ close'
   readIORef dbm >>= mapM_ closeM

insertDB :: DB -> DeviceID -> DBEntry -> IO ()
insertDB db dev entry = setDB db dev >>= mapM_ (insert' entry)

updateDB :: DB -> DeviceID -> DBEntry -> IO ()
updateDB db dev entry = setDB db dev >>= mapM_ (update' entry)

getDB :: DB -> DeviceID -> DBKey -> IO (Maybe DBEntry)
getDB db dev key = fmap join $ setDB db dev >>= mapM (get' key)

deleteDB :: DB -> DeviceID -> DBKey -> IO ()
deleteDB db dev key = setDB db dev >>= mapM_ (delete' key)

getTarget :: DB -> DeviceID -> IO (Maybe FilePath)
getTarget db dev = join . fmap _mntTarget <$> setDB db dev

getHID :: DB -> BS.ByteString -> IO Int64
getHID db hash = join $ flip getHID' hash <$> getM db

resetHID :: DB -> IO ()
resetHID = getM >=> resetHID'

createDBDirectory :: FilePath -> IO ()
createDBDirectory path' = do
  let path = dropTrailingPathSeparator path'
      create dir = do
        (e, rw) <- checkDir dir
        case (e, rw) of
          (_, True)     -> putStrLn $ "cache directory already exists at \"" ++ dir ++ "\""
          (True, False) -> putStrLn $ "cache directory already exists at \"" ++ dir ++
                                      "\"\n but is not readable or writable"
          _ -> do createDirectoryIfMissing False dir
                  putStrLn $ "created directory \"" ++ dir ++ "\"\n (will be used when readable & writable)"
  if path == "/var/cache"
    then create _globalDir
  else do
    p <- canonicalizePath path
    mps <- filter (isJust . Mnt.uuid) <$> Mnt.points
    case find ((== p) . Mnt.target) mps of
      Nothing -> hPutStrLn stderr $ "error: " ++ path' ++ " is not /var/cache nor a moint point of a device with UUID"
      Just _  -> create $ mkMntPointDir p


-- * internal

-- ** DB directories

_globalDir :: FilePath
_globalDir = "/var/cache/fh-" ++ fhID

mkMntPointDir :: FilePath -> FilePath
mkMntPointDir root = root </> ".fh-" ++ fhID

checkDir :: FilePath -> IO (Bool, Bool)
checkDir dir = do
  e <- doesDirectoryExist dir
  rw <- if e
          then do p <- getPermissions dir
                  return $ readable p && writable p
          else return False
  return (e, rw)

globalDir :: Maybe Mnt.Point -> DeviceID -> IO (Maybe FilePath)
globalDir mp' dev = do
  case mp' of
    Just mp -> do let mpDir = mkMntPointDir (Mnt.target mp)
                  (_, rw) <- checkDir mpDir
                  if rw then return $ Just mpDir
                        else globalDir Nothing dev
    Nothing -> do
      (_, rw) <- do checkDir _globalDir
      return $ if rw then Just _globalDir else Nothing

userDir :: IO FilePath
userDir = do
  dir <- getXdgDirectory XdgCache ("fh-" ++ fhID)
  createDirectoryIfMissing True dir
  return dir


-- ** DB'

data DB' = DB' { _dev       :: DeviceID
               , _mntTarget :: Maybe FilePath -- Nothing when filesystem supports inodes
               , _path      :: FilePath
               , _DB        :: Database
               , _ins       :: Statement
               , _upd       :: Statement
               , _get       :: Statement
               , _del       :: Statement
               }

setDB :: DB -> DeviceID -> IO (Maybe DB')
setDB (DB mps dbR _) dev = do
  db_' <- readIORef dbR
  case db_' of
    Nothing -> setnew
    Just db' | dev == _dev db' -> return $ Just db'
             | otherwise       -> close' db' >> setnew
    where setnew = do
            let mp' = find ((== dev) . Mnt.devid) mps
            dir <- fromMaybe <$> userDir <*> globalDir mp' dev
            db'' <-
              case do mp <- mp'
                      uuid <- Mnt.uuid mp :: Maybe String
                      let path   = dir </> uuid <.> "v" ++ show version <.> "db"
                          hasIno = Mnt.fstype mp `elem` ["ext2", "ext3", "ext4"]
                      return (path, if hasIno then Nothing else Just $ Mnt.target mp)
              of Nothing             -> return Nothing
                 Just (path, target) -> Just <$> open' dev target path
            writeIORef dbR db''
            return db''

open' :: DeviceID -> Maybe FilePath -> FilePath -> IO DB'
open' dev target path = do
  db <- open $ fromString path
  exec db  " PRAGMA busy_timeout = 100000;      \
           \ CREATE TABLE IF NOT EXISTS files ( \
           \   key   INTEGER PRIMARY KEY,       \
           \   utime INTEGER,                   \
           \   size  INTEGER,                   \
           \   du    INTEGER,                   \
           \   cnt   INTEGER,                   \
           \   sha1  BLOB,                      \
           \   hpath BLOB                     ) "
  -- utime: update time
  exec db "BEGIN TRANSACTION"
  DB' dev target path db
    <$> prepare db "INSERT INTO files values (?, ?, ?, ?, ?, ?, ?)"
    <*> prepare db "UPDATE files SET utime=?, size=?, du=?, cnt=?, sha1=?, hpath=? WHERE key=?"
    <*> prepare db "SELECT * FROM files WHERE key=?"
    <*> prepare db "DELETE   FROM files WHERE key=?"

close' :: DB' -> IO ()
close' db = do
    exec (_DB db) "END TRANSACTION"
    mapM_ finalize [_ins db, _upd db, _get db, _del db]
    close (_DB db)

int :: Integral a => a -> SQLData
int = SQLInteger . fromIntegral

insert' :: DBEntry -> DB' -> IO ()
insert' (key, utime, size, du, cnt, sha1, hpath) db = do
  let ins = _ins db
  bind ins [int key, int utime, int size, int du, int cnt, SQLBlob sha1, SQLBlob hpath]
  _ <- step ins
  reset ins
  restartTx db

update' :: DBEntry -> DB' -> IO ()
update' (key, utime, size, du, cnt, sha1, hpath) db = do
  let upd = _upd db
  bind upd [int utime, int size, int du, int cnt, SQLBlob sha1, SQLBlob hpath, int key]
  _ <- step upd
  reset upd
  restartTx db

get' :: DBKey -> DB' -> IO (Maybe DBEntry)
get' key db = do
  let get = _get db
  reset get
  bindInt64 get 1 key
  res <- step get
  case res of
    Done -> return Nothing
    Row -> do
      [_, SQLInteger utime, SQLInteger size, SQLInteger du, SQLInteger cnt, SQLBlob sha1, SQLBlob hpath] <-
        typedColumns get $ replicate 5 (Just IntegerColumn) ++ [Just BlobColumn, Just BlobColumn]
      return $ Just (key, utime, fromIntegral size, fromIntegral du, fromIntegral cnt, sha1, hpath)

delete' :: DBKey -> DB' -> IO ()
delete' key db = do
  let del = _del db
  bindInt64 del 1 key
  _ <- step del
  reset del
  restartTx db

restartTx :: DB' -> IO ()
restartTx db = do
  x <- randomIO :: IO Int8
  when (x == 0) $ do
    exec (_DB db) "end transaction"
    delay 1000
    exec (_DB db) "begin transaction"


-- ** Hash ID

data IDMap = IDMap { _DBM  :: Database
                   , _cntM :: Statement
                   , _selM :: Statement
                   , _insM :: Statement
                   }

getM :: DB -> IO IDMap
getM (DB _ _ dbR) = do
  dbm' <- readIORef dbR
  case dbm' of
    Nothing -> do
      dbm <- openM
      writeIORef dbR $ Just dbm
      return dbm
    Just dbm -> return dbm

openM :: IO IDMap
openM = do
  db <- open $ fromString $ "/tmp/fh-IDMap-" ++ fhID ++ ".db"
  exec db " CREATE TABLE IF NOT EXISTS [idmap] ( \
          \   sha1 BLOB PRIMARY KEY,             \
          \   id   INTEGER UNIQUE              ) "
  exec db "BEGIN TRANSACTION"
  IDMap db
    <$> prepare db "SELECT MAX(_ROWID_) FROM [idmap] LIMIT 1"
    <*> prepare db "SELECT id FROM [idmap] WHERE sha1=?"
    <*> prepare db "INSERT INTO [idmap] values (?, ?)"

closeM :: IDMap -> IO ()
closeM db = do
  exec (_DBM db) "END TRANSACTION"
  mapM_ finalize [_cntM db, _selM db, _insM db]
  close (_DBM db)

getHID' :: IDMap -> BS.ByteString -> IO Int64
getHID' db hash = do
  let sel = _selM db
  reset sel
  bindBlob sel 1 hash
  res <- step sel
  case res of
    Done -> do
      _ <- step (_cntM db)
      [SQLInteger cnt] <- typedColumns (_cntM db) [Just IntegerColumn]
      reset (_cntM db)
      let ins = _insM db
      bind ins [SQLBlob hash, SQLInteger (cnt+1)]
      _ <- step ins
      reset ins
      return $ cnt+1
    Row -> do
      [SQLInteger hid] <- typedColumns sel [Just IntegerColumn]
      return hid

resetHID' :: IDMap -> IO ()
resetHID' db = do
  del <- prepare (_DBM db) "DELETE FROM [idmap]"
  _ <- step del
  finalize del
