module Stat where

import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr        (plusPtr)
import Foreign.Storable   (peek)
import System.IO.Unsafe   (unsafePerformIO)
import System.Posix.Files (FileStatus)
import Unsafe.Coerce      (unsafeCoerce)

-- cf. https://ghc.haskell.org/trac/ghc/ticket/7345
fileBlockSize :: FileStatus -> Int
fileBlockSize s = unsafePerformIO $
                    withForeignPtr (unsafeCoerce s) $
                      peek . (`plusPtr` 64)
                      -- TODO: check to which extent 64 is valid
