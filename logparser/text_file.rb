# Copyright (c) 2012 Xero Ltd
# 
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
# 
# The above copyright notice and this permission notice shall
# be included in all copies or substantial portions of the
# Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

require 'zlib'
require 'logparser/logger'

module LogParser

  class TextFile
    attr_reader :handle, :pos, :eof
    def initialize handle
      handle.binmode if handle.respond_to? :binmode
      @handle = handle
      @buffer = ''
      @pos = 0
      @eof = false
      @first = true
    end

    def pos= pos
      @pos = _seek pos
      throw "SEEK FAILED" unless @pos == pos
      @buffer = ''
      pos
    end

    def gets
      if @first
        # none of our lines are more than a few hundred characters long so
        # if there are any line endings, they should appear in the first 16K
        _read 16384
        if @buffer =~ /\r\n/
          @regexp = /^([^\r\n\x1a]*)\r\n/
        else
          @regexp = /^([^\n\x1a]*)\n/
        end
        @first = false
      end

      if @buffer =~ @regexp
        @pos = @pos + $~.end(0)
        line = $1
        @buffer = @buffer[$~.end(0), @buffer.length]
        return line unless line == ""
        log :error, "EMPTY LINE: #{@pos}"
        return false
      end

      if @buffer =~ /[\x1a]/
        @eof = true
      end

      return false if @eof

      if @buffer.length > 16384
        log :error, "ERROR: BUFFER IS ENORMOUS"
        return false
      end

      unless _read 16384
        return false
      end

      return gets
    end

    def _read bytes
      begin
        data = @handle.sysread bytes
        @buffer = @buffer + data
        return true
      rescue EOFError
        @eof = true
        return false
      end
    end

    def _seek pos
      @handle.sysseek pos
    end

    def close
      # no-op
    end

  end

  class GzipTextFile < TextFile

    def initialize handle
      handle.binmode if handle.respond_to? :binmode
      @orig_handle = handle
      gzip_handle = Zlib::GzipReader.new(handle)
      super gzip_handle
    end

    def _read bytes
      begin
        data = @handle.read bytes
        unless data.nil?
          @buffer = @buffer + data
          return true
        end
      rescue
      end
      @eof = true
      return false
    end

    def _seek pos
      if pos == @pos
        return pos
      end
      raise "Cannot seek in gzip stream"
    end

    def close
      @handle.close
    end

  end

end