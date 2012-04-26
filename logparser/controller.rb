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

require 'logparser/text_file'
require 'logparser/logger'
require 'pp'

module LogParser

  class Controller
    include Logger

    attr_reader :checkpoint_init, :total_lines

    def initialize sink
      @sink = sink
      @checkpoint = {}
      @checkpointing = false
      @checkpoint_init = false
      @total_lines = 0
    end

    def setup_checkpoint file, init = false
      @checkpointing = true
      @checkpoint_file = file
      @checkpoint_init = init
      unless @checkpoint_init
        @checkpoint = nil
        if File.file? @checkpoint_file
          open(@checkpoint_file, "r") do |f|
            data = f.read
            if @checkpoint_file =~ /\.marshal/
              @checkpoint = Marshal.load data
            else
              @checkpoint = eval data
            end
          end
        end
        @checkpoint_init = @checkpoint.nil?
        @checkpoint = {} if @checkpoint.nil?
      end
    end

    def checkpoint_write
      open(@checkpoint_file+".tmp", "w") do |f|
        if @checkpoint_file =~ /\.marshal/
          f.write(Marshal.dump(@checkpoint))
        else
          PP.pp(@checkpoint, f)
        end
      end
      begin
        File.rename @checkpoint_file+".tmp", @checkpoint_file
      rescue
        File.unlink @checkpoint_file
        File.rename @checkpoint_file+".tmp", @checkpoint_file
      end
    end

    def read *globs
      files = []
      globs.each do |glob|
        if glob =~ /(_STAR_|\*)/
          glob = glob.gsub(/_STAR_/,'*')
          files = files + Dir.glob(glob)
        else
          files.push glob
        end
      end

      return if files.empty?

      files.sort!

      if @checkpoint_init
        last_fn = nil
        last_dn = nil
        list = []
        files.each do |fn|
          dn = File.dirname(fn)
          if last_dn != dn && !last_fn.nil?
            list.push last_fn
          end
          last_fn = fn
          last_dn = dn
        end
        files.each { |x| @checkpoint[x] = { :ignore => true } unless list.include? x }
        files = list
      end
      
      files.each do |fn|
        read_file fn
      end
    end

    def stat
      pp @checkpoint
    end

    private

    def read_file fn
      # parse state
      checkpoint = {}
      if @checkpoint.has_key? fn
        if @checkpoint[fn] == :ignore
          @checkpoint[fn] = { :ignore => true }
        end
        @checkpoint[fn][:seen] = Time.new.to_i
        return if @checkpoint[fn][:ignore] == true
        checkpoint = @checkpoint[fn].clone
        seek = checkpoint[:seek]
        unless seek.nil?
          if File.size(fn) <= seek
            return
          end
        end
      end
      pos = nil
      prev_pos = nil
      log :info, "reading #{fn}"
      @sink.logfile = fn
      start = Time.new
      lines = 0
      open(fn,"rb") do |handle|
        file = nil
        if fn =~ /\.gz$/
          file = GzipTextFile.new handle
        else
          file = TextFile.new handle
        end
        if @checkpointing
          unless checkpoint[:fields].nil?
            @sink.fields = checkpoint[:fields]
            log :debug, "init fields #{checkpoint[:fields].join ", "}"
          end
          unless checkpoint[:seek].nil?
            pos = checkpoint[:seek]
            prev_pos = pos
            file.pos = pos
            log :debug, "seek to #{pos}"
          end
        end
        continue = true

        while (line = file.gets) && continue
          prev_pos = pos
          lines = lines.next
          @total_lines = @total_lines.next
          pos = file.pos
          continue = @sink.write line
        end
        # if continue false, we pretend we didn't parse the last line
        unless continue
          pos = prev_pos
        end
        if @checkpointing
          checkpoint[:seek] = pos unless pos.nil?
          fields = @sink.fields
          checkpoint[:fields] = fields unless fields.nil?
        end
        file.close
      end
      time = Time.new - start
      time = 0.1 if time == 0
      log :perf, "#{lines} lines in #{time}s, #{(lines/time).to_i} l/s"
      @checkpoint[fn] = checkpoint
    end

  end


end
