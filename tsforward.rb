require 'rubygems'
require 'socket'
require 'eventmachine'

unless ARGV.length == 4
  puts "Usage: tsforward.rb <listen_addr> <listen_port> <backend_addr> <backend_port>\n"
  exit
end

class Tsforward < EM::Protocols::LineAndTextProtocol

  def initialize
    @buf        = []
    @port, @ip  = Socket.unpack_sockaddr_in(get_peername)
  end

  def receive_data(data)
    @buf << data
    forward_metric(@buf.to_s) if validate_metric?(@buf.to_s)
    reset_buf()
  end
  
  def validate_metric?(data)
    command = /^\["put(\s\w.+\s\d+\s\d)/
    !!data.match(command)
  end

  def forward_metric(data)
    conn = TCPSocket.new(ARGV[2], ARGV[3])
    conn.send data, 4096
    conn.close
  end

  def unbind
    @log.info("#{self.class.to_s} disconnected for #{@ip}:#{@port}")
  end

  private
  def reset_buf
    @buf = []
  end
end

EventMachine::run {
  EventMachine::start_server ARGV[0], ARGV[1], Tsforward
}
