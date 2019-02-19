{-# LANGUAGE OverloadedStrings #-}

module DB (DB, closeDB, getDB, getHID, insertDB, newDB, updateDB) where

import           Control.Monad    (forM, forM_, join, mapM_, (>=>))
import qualified Data.ByteString  as BS
import           Data.Int         (Int64)
import           Data.IORef
import           Data.String      (fromString)
import           Database.SQLite3 (ColumnType (..), Database, SQLData (..), Statement,
                                   StepResult (..), bind, bindBlob, bindInt64, close, exec,
                                   finalize, open, prepare, reset, step, typedColumns)

-- * public API

type DB = IORef (Maybe DB', Maybe IDMap)
type Entry = (Int64, Int64, Int, Int, BS.ByteString)

newDB :: IO DB
newDB = newIORef (Nothing, Nothing)

closeDB :: DB -> IO ()
closeDB db = do
  (db', dbm) <- readIORef db
  mapM_ close' db'
  mapM_ closeM dbm

insertDB :: DB -> Maybe FilePath -> Entry -> IO ()
insertDB db path entry = forM_ path $ setDB db >=> insert' entry

updateDB :: DB -> Maybe FilePath -> Entry -> IO ()
updateDB db path entry = forM_ path $ setDB db >=> update' entry

getDB :: DB -> Maybe FilePath -> Int64 -> IO (Maybe Entry)
getDB db path key = fmap join . forM path $ setDB db >=> get' key

getHID :: DB -> BS.ByteString -> IO Int64
getHID db hash = join $ flip getHID' hash <$> getM db


-- * internal

-- ** DB'

data DB' = DB' { _path :: FilePath
               , _DB   :: Database
               , _ins  :: Statement
               , _upd  :: Statement
               , _get  :: Statement
               }

setDB :: DB -> FilePath -> IO DB'
setDB db path = do
  (db_, dbm) <- readIORef db
  case db_ of
    Nothing -> setnew dbm
    Just db' | path == _path db' -> return db'
             | otherwise         -> close' db' >> setnew dbm
    where setnew dbm = do
            db'' <- open' path
            writeIORef db (Just db'', dbm)
            return db''

open' :: FilePath -> IO DB'
open' path = do
  db <- open $ fromString path
  exec db  " CREATE TABLE IF NOT EXISTS files ( \
           \   key   INTEGER PRIMARY KEY,       \
           \   utime INTEGER,                   \
           \   size  INTEGER,                   \
           \   du    INTEGER,                   \
           \   sha1  BLOB                     ) "
  -- utime: update time
  exec db "BEGIN TRANSACTION"
  DB' path db
    <$> prepare db "INSERT INTO files values (?, ?, ?, ?, ?)"
    <*> prepare db "UPDATE files SET utime=?, size=?, du=?, sha1=? WHERE key=?"
    <*> prepare db "SELECT * FROM files WHERE key=?"

close' :: DB' -> IO ()
close' db = do
    exec (_DB db) "END TRANSACTION"
    mapM_ finalize [_ins db, _upd db, _get db]
    close (_DB db)

int :: Integral a => a -> SQLData
int = SQLInteger . fromIntegral

insert' :: Entry -> DB' -> IO ()
insert' (key, utime, size, du, sha1) db = do
  let ins = _ins db
  bind ins [int key, int utime, int size, int du, SQLBlob sha1]
  _ <- step ins
  reset ins

update' :: Entry -> DB' -> IO ()
update' (key, utime, size, du, sha1) db = do
  let upd = _upd db
  bind upd [int utime, int size, int du, SQLBlob sha1, int key]
  _ <- step upd
  reset upd

get' :: Int64 -> DB' -> IO (Maybe Entry)
get' key db = do
  let get = _get db
  reset get
  bindInt64 get 1 key
  res <- step get
  case res of
    Done -> return Nothing
    Row -> do
      [_, SQLInteger utime, SQLInteger size, SQLInteger du, SQLBlob sha1] <-
        typedColumns get $ replicate 4 (Just IntegerColumn) ++ [Just BlobColumn]
      return $ Just (key, utime, fromIntegral size, fromIntegral du, sha1)


-- ** Hash ID

data IDMap = IDMap { _DBM  :: Database
                   , _cntM :: Statement
                   , _selM :: Statement
                   , _insM :: Statement
                   }

getM :: DB -> IO IDMap
getM db = do
  (db_, dbm') <- readIORef db
  case dbm' of
    Nothing -> do
      dbm <- openM
      writeIORef db (db_, Just dbm)
      return dbm
    Just dbm -> return dbm

openM :: IO IDMap
openM = do
  db <- open "/tmp/fh-IDMap-XSfaWmMBsldaiatb9rjmMwKers.db"
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
