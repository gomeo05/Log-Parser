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

class OptParser
  def initialize argv = ARGV, multi_opts = [], map_opts = []
    @flags = []
    @params = []
    @options = { }
    @multi_opts = []
    @map_opts = []
    add_multi *multi_opts
    add_map *map_opts
    @processed = false
    @argv = argv.clone
    self
  end
  
  def add_multi *args
    raise "can't change options, args already parsed" if @processed
    args.each do |arg|
      @multi_opts.push arg
      @options[arg] = []
    end
  end
  
  def add_map *args
    raise "can't change options, args already parsed" if @processed
    args.each do |arg|
      @map_opts.push arg
      @options[arg] = {}
    end
  end

  def lazy_process
    unless @processed
      process @argv
      @processed = true
    end
  end

  def flag? x
    lazy_process
    @flags.include? x
  end

  def has_option? x
    lazy_process
    @options.has_key? x
  end

  def option x
    lazy_process
    @options[x]
  end

  def params
    lazy_process
    @params
  end

  def options
    lazy_process
    @options
  end

  def [](x)
    if has_option? x
      option x
    elsif flag? x
      true
    else
      nil
    end
  end

  def []=(key, value)
    lazy_process
    if value == false
      @flags.reject! { |x| x == key }
    elsif value == true
      @flags.push key unless @options.has_key? key
    else
      @flags.reject! { |x| x == key }
      @options.delete key
      @options[key] = value unless value.nil?
    end
  end

  private

  def process list
    list.each do |x|
      if x =~ /^@(.*)$/
        params = []
        File.open($1, "rb") do |file|
          file.each do |line|
            line.untaint
            line.sub!(/^\357\273\277/,"")
            line.strip!
            line.sub!(/\#.*$/,'') unless line.nil?
            line.strip!
            unless line.nil? || (line == "")
              params.push line
            end
          end
        end
        process params
      elsif x =~ /^--([^=]+)=(.*)$/
        value = $2
        opt = $1.gsub(/-/,"_").to_sym
        if @map_opts.include? opt
          if value =~ /^([^=]+)=(.*)$/
            value = $2
            key = $1.gsub(/-/,"_").to_sym
            @options[opt][key] = value
          else
            raise "option #{x} is not parseable"
          end
        elsif @multi_opts.include? opt
          @options[opt].push value
        else
          @options[opt] = value
        end
      elsif x =~ /^--(.*)$/
        @flags.push $1.gsub(/-/,"_").to_sym
      else
        @params.push x
      end
    end
  end

end