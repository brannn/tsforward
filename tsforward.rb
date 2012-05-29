require 'socket'
require 'eventmachine'
require 'logger'

unless ARGV.length == 4
  puts "Usage: tsforward.rb <listen_addr> <listen_port> <backend_addr> <backend_port>\n"
  exit
end

class Tsforward < EM::Protocols::LineAndTextProtocol

  def initialize
    @buf        = []
    @port, @ip  = Socket.unpack_sockaddr_in(get_peername)
    @log        = Logger.new(STDOUT)
    @log.info("New #{self.class.to_s} for #{@ip}:#{@port}")
  end

  def receive_data(data)
    @buf << data
    @log.info("Received #{data.length} bytes from #{@ip}:#{@port}")
    forward_metric(@buf.to_s) if validate_metric?(@buf.to_s)
    reset_buf()
    @log.info("Reset #{self.class.to_s} buffer for #{@ip}:#{@port}")
  end
  
  def validate_metric?(data)
    command = /^\["put(\s\w.+\s\d+\s\d)/
    !!data.match(command)
  end

  def forward_metric(data)
    conn = TCPSocket.new(ARGV[2], ARGV[3])
    conn.send data, 1024
    @log.info("Sent #{data.length} bytes to #{ARGV[2]}:#{ARGV[3]}")
    conn.close
  end

  def unbind
    @log.info("#{self.class.to_s} #{@ip}:#{@port} disconnected")
  end

  private
  def reset_buf
    @buf = []
  end
end

EventMachine::run {
  EventMachine::start_server ARGV[0], ARGV[1], Tsforward
}
