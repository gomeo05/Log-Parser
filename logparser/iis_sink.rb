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

require 'logparser/sink'

module LogParser
  class IISSink < Sink
    def initialize fn, *args
      @fn = fn
      @fh = nil
      super *args
    end

    def flush
      if @fn !~ /\.gz$/
        unless @fh.nil?
          @fh.close
          @fh = nil
        end
      end
      super
    end

    def close
      unless @fh.nil?
        @fh.close
        @fh = nil
      end
      super
    end

    def fields_filter fields
      fh.puts "\#Fields: #{fields.join ' '}"
      true
    end

    def write_filter data
      fh.puts data.join(' ')
      true
    end

    private

    def fh
      if @fh.nil?
        if @fn =~ /\.gz$/
          backing_fh = File.open @fn, "w"
          backing_fh.binmode if backing_fh.respond_to? :binmode
          @fh = Zlib::GzipWriter.new(backing_fh)
        else
          @fh = File.open @fn, "a"
        end
      end
      @fh
    end

  end
end