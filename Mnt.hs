module Mnt where

import Data.Int        (Int64)
import Data.List       (isSuffixOf)
import Data.List.Split (splitOn)
import System.Process  (readProcess)

data Point = Point { target  :: FilePath
                   , fstype  :: String
                   , options :: String
                   , devid   ::Int64
                   , uuid    :: Maybe String
                   , bind    :: Maybe FilePath
                   } deriving (Show)


points :: IO [Point]
points = map parseMnt . lines <$>
         readProcess "findmnt" ["--raw", "--noheadings", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS,MAJ:MIN,UUID"] ""

parseMnt :: String -> Point
parseMnt mnt = Point _target _fstype _options _devid _uuid _bind
  where _target:_source':_fstype:_options:majmin:_uuid' = words mnt
        imaj:imin:_ = map read (splitOn ":" majmin)
        _devid = imaj * 2^(8::Int) + imin
        (_source, _bind) = if "]" `isSuffixOf` _source' && '[' `elem` _source'
                           then let s:b:_ = splitOn "[" _source'
                                in (s, Just $ init b)
                           else (_source', Nothing)
        _uuid = if null _uuid' then Nothing else Just $ head _uuid'
