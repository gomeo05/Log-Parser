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
  class TimeCutoff < Sink
    def initialize cutoff, sink = nil
      super sink
      @cutoff = cutoff
      @early = cutoff - 360
      @oldc = 0
    end

    def write data
      # date time format
      # 2010-07-29 00:00:21
      stamp = data[@fieldmap['date']].split(/-/) + data[@fieldmap['time']].split(/:/)
      result = nil
      old = nil
      begin
        ltime = Time.utc(*stamp)
        result = @cutoff > ltime
        old = ltime <= @early
      rescue => e
        puts "TIME FAILURE"
        puts data[@fieldmap['date']]
        puts data[@fieldmap['time']]
        puts stamp.join(',')
        puts data.join(' ')
        puts e
        throw "AAAAARGH"
      end
      if old
        @oldc += 1
        return true
      end
      if @oldc > 0
        puts "OLD SKIPPED: #{@oldc}"
        puts "NOW OK #{stamp.join('-')} #{@early} #{@cutoff}"
        @oldc = 0
      end
      if result
        write_next data
      else
        false
      end
    end

    def fields= fields
      if @oldc > 0
        puts "OLD SKIPPED: #{@oldc}"
        @oldc = 0
      end
      super
    end
  end
end
