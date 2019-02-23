{-# LANGUAGE OverloadedStrings #-}

module DB (DB, DBEntry, DBKey, closeDB, createDBDirectory, getDB,
           getHID, getTarget, insertDB, newDB, resetHID, updateDB) where

import           Control.Monad      (join, mapM_, (>=>))
import qualified Data.ByteString    as BS
import           Data.Int           (Int64)
import           Data.IORef
import           Data.List          (find)
import           Data.Maybe         (fromMaybe, isJust)
import           Data.String        (fromString)
import           Database.SQLite3   (ColumnType (..), Database, SQLData (..), Statement,
                                     StepResult (..), bind, bindBlob, bindInt64, close, exec,
                                     finalize, open, prepare, reset, step, typedColumns)
import           System.Directory   (XdgDirectory (XdgCache), createDirectoryIfMissing,
                                     doesDirectoryExist, getPermissions, getXdgDirectory, readable,
                                     writable)
import           System.FilePath    ((<.>), (</>))
import           System.Posix.Types (DeviceID)

import qualified Mnt


fhID :: String
fhID = "XSfaWmMBsldaiatb9rjmMwKers"

version :: Int
version = 0


-- * public API

type DBKey = Int64
data DB = DB [Mnt.Point] (IORef (Maybe DB')) (IORef (Maybe IDMap))
type DBEntry = (DBKey, Int64, Int, Int, Int, BS.ByteString, BS.ByteString)


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

getTarget :: DB -> DeviceID -> IO (Maybe FilePath)
getTarget db dev = join . fmap _mntTarget <$> setDB db dev

getHID :: DB -> BS.ByteString -> IO Int64
getHID db hash = join $ flip getHID' hash <$> getM db

resetHID :: DB -> IO ()
resetHID = getM >=> resetHID'

createDBDirectory :: IO ()
createDBDirectory = do
  (e, rw) <- checkGlobalDir
  case (e, rw) of
    (_, True)     -> putStrLn $ "cache directory already exists at \"" ++ _globalDir ++ "\""
    (True, False) -> putStrLn $ "cache directory already exists at \"" ++ _globalDir ++
                                "\"\n but is not readable or writable"
    _ -> do createDirectoryIfMissing False _globalDir
            putStrLn $ "created directory \"" ++ _globalDir ++ "\"\n (will be used when readable & writable)"


-- * internal

_globalDir :: FilePath
_globalDir = "/var/cache/fh-" ++ fhID

checkGlobalDir :: IO (Bool, Bool)
checkGlobalDir = do
  e <- doesDirectoryExist _globalDir
  rw <- if e
          then do p <- getPermissions _globalDir
                  return $ readable p && writable p
          else return False
  return (e, rw)

globalDir :: IO (Maybe FilePath)
globalDir = do
  (_, rw) <- checkGlobalDir
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
               }

setDB :: DB -> DeviceID -> IO (Maybe DB')
setDB (DB mps dbR _) dev = do
  db_' <- readIORef dbR
  case db_' of
    Nothing -> setnew
    Just db' | dev == _dev db' -> return $ Just db'
             | otherwise       -> close' db' >> setnew
    where setnew = do
            dir <- fromMaybe <$> userDir <*> globalDir
            case do mp <- find ((== dev) . Mnt.devid) mps
                    uuid <- Mnt.uuid mp :: Maybe String
                    let path   = dir </> uuid <.> "v" ++ show version <.> "db"
                        hasIno = Mnt.fstype mp `elem` ["ext2", "ext3", "ext4"]
                    return (path, if hasIno then Nothing else Just $ Mnt.target mp)
              of Nothing -> return Nothing
                 Just (path, target) -> do
                   db'' <- open' dev target path
                   writeIORef dbR $ Just db''
                   return $ Just db''

open' :: DeviceID -> Maybe FilePath -> FilePath -> IO DB'
open' dev target path = do
  db <- open $ fromString path
  exec db  " CREATE TABLE IF NOT EXISTS files ( \
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

close' :: DB' -> IO ()
close' db = do
    exec (_DB db) "END TRANSACTION"
    mapM_ finalize [_ins db, _upd db, _get db]
    close (_DB db)

int :: Integral a => a -> SQLData
int = SQLInteger . fromIntegral

insert' :: DBEntry -> DB' -> IO ()
insert' (key, utime, size, du, cnt, sha1, hpath) db = do
  let ins = _ins db
  bind ins [int key, int utime, int size, int du, int cnt, SQLBlob sha1, SQLBlob hpath]
  _ <- step ins
  reset ins

update' :: DBEntry -> DB' -> IO ()
update' (key, utime, size, du, cnt, sha1, hpath) db = do
  let upd = _upd db
  bind upd [int utime, int size, int du, int cnt, SQLBlob sha1, SQLBlob hpath, int key]
  _ <- step upd
  reset upd

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
    <$> prepare db "SELECT COUNT(*) FROM [idmap]"
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
