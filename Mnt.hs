module Mnt where

import Control.Monad      (foldM, mapM)
import Data.List          (isSuffixOf)
import Data.List.Split    (splitOn)
import System.Directory   (canonicalizePath, listDirectory)
import System.FilePath    ((</>))
import System.Posix.Files (readSymbolicLink)
import System.Posix.Types (DeviceID)
import System.Process     (readProcess)


data Point = Point { target  :: FilePath
                   , source  :: FilePath
                   , fstype  :: String
                   , options :: String
                   , devid   :: DeviceID
                   , uuid    :: Maybe String
                   , bind    :: Maybe FilePath
                   } deriving (Show)


points :: IO [Point]
points = do
  pts <- map parseMnt . lines <$>
         readProcess "findmnt" ["--raw", "--noheadings", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS,MAJ:MIN,UUID"] ""
  if all hasNoUUID pts
    then do
      uuids <- getUUIDs
      mapM (addUUID uuids) pts
    else return pts
  where hasNoUUID pt = null $ uuid pt
        addUUID uuids pt = do
          src <- canonicalizePath $ source pt
          return $ pt { uuid = lookup src uuids }


parseMnt :: String -> Point
parseMnt mnt = Point _target _source _fstype _options _devid _uuid _bind
  where _target:_source':_fstype:_options:majmin:_uuid' = words mnt
        imaj:imin:_ = map read (splitOn ":" majmin)
        _devid = imaj * 2^(8::Int) + imin
        (_source, _bind) = if "]" `isSuffixOf` _source' && '[' `elem` _source'
                           then let s:b:_ = splitOn "[" _source'
                                in (s, Just $ init b)
                           else (_source', Nothing)
        _uuid = if null _uuid' then Nothing else Just $ head _uuid'

getUUIDs :: IO [(FilePath, String)]
getUUIDs = do
  values <- listDirectory byuuid
  foldM addTarget [] values
    where byuuid = "/dev/disk/by-uuid"
          addTarget list _uuid = do
            target' <- readSymbolicLink $ byuuid </> _uuid
            target'' <- canonicalizePath $ byuuid </> target'
            return $ (target'', _uuid):list
