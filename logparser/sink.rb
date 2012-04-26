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

require 'logparser/logger'

module LogParser

  class Sink
    include Logger
    attr_reader :fieldmap, :fields, :logfile

    def initialize sink = nil
      @sink = sink
      @fields = nil
      @fieldmap = nil
      self
    end

    def next= sink
      @sink = sink
    end

    def logfile= logfile
      @logfile = logfile
      @sink.logfile = logfile unless @sink.nil?
    end

    def fields= fields
      return false unless self.fields_filter fields
      unless @sink.nil?
        return false unless @sink.fields = fields
      end
      i = 0
      @fieldmap = {}
      fields.each { |x| @fieldmap[x] = i ; i = i.next }
      @fields = fields.clone
      true
    end

    def fields_filter fields
      true
    end

    def write data
      return false unless write_filter data
      write_next data
    end

    def write_next data
      unless @sink.nil?
        return @sink.write(data)
      end
      true
    end

    def flush
      unless @sink.nil?
        @sink.flush
      end
    end

    def close
      unless @sink.nil?
        @sink.close
      end
    end

  end

end
