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
  class IISParser < Sink
    def initialize *args
      super *args
      @unknown_limit = 200
    end

    def write line
      if line =~ /^\#/
        if line =~ /^\#Fields: (.*)/
          self.fields = $1.split(/ /)
        else
          # ignore
          true
        end
      elsif @fields.nil?
        if @unknown_limit > 0
          @unknown_limit -= 1
          log :error, 'SKIPPING DUE TO NO FIELDS'
          true
        else
          log :error, 'Reached limit of lines with no field headings'
        end
      else
        data = line.split(/ /)
        if data.size != @fields.size
          log :error, 'PARSE ERROR'
          log :error, line
          log :error, line.length
          return false
        end
        write_next data
      end
    end
  end
end
