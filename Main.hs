module Main where

import Options.Applicative (Parser, ParserInfo, argument, execParser, fullDesc,
                            help, helper, info, long, many, metavar, progDesc,
                            short, str, switch, (<**>))


main :: IO ()
main = do
  o <- execParser options
  return ()


-- * Options

data Options = Options { optSHA1  :: Bool
                       , optSize  :: Bool
                       , optTotal :: Bool

                       , optSI    :: Bool
                       , optSort  :: Bool

                       , optPaths :: [FilePath]
                       }

parserOptions :: Parser Options
parserOptions = Options
                <$> switch (long "sha1" <> short 'x' <>
                            help "print sha1 hash (in hexadecimal)")
                <*> switch (long "size" <> short 's' <>
                            help "print (apparent) size")
                <*> switch (long "total" <> short 'c' <>
                            help "produce a grand total")

                <*> switch (long "si" <> short 't' <>
                            help "use powers of 1000 instead of 1024 for sizes")
                <*> switch (long "sort" <> short 'S' <>
                            help "sort output, according to size")

                <*> many (argument str (metavar "PATHS..."))

options :: ParserInfo Options
options = info (parserOptions <**> helper)
          $  fullDesc
          <> progDesc "compute and cache the sha1 hash and size of files and directories"
