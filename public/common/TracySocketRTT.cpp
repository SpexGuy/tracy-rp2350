#include "TracySocket.hpp"

extern "C" void tracy_rtt_write_blocking(const void *data, int len);
extern "C" int tracy_rtt_get_max_write_size();
extern "C" int tracy_rtt_read_nonblocking(void *data, int len);
extern "C" bool tracy_rtt_read_all_or_nothing_blocking(void* buf, int len, int timeout);
extern "C" bool tracy_rtt_read_blocking(void *data, int len, int timeout);
extern "C" bool tracy_rtt_has_data();

namespace tracy
{

Socket::Socket()
    : m_buf( nullptr )
    , m_bufPtr( nullptr )
    , m_sock( -1 )
    , m_bufLeft( 0 )
    , m_ptr( nullptr )
{
}

Socket::Socket( int sock )
    : m_buf( nullptr )
    , m_bufPtr( nullptr )
    , m_sock( sock )
    , m_bufLeft( 0 )
    , m_ptr( nullptr )
{
}

Socket::~Socket()
{
}

bool Socket::Connect( const char* addr, uint16_t port )
{
    m_sock = 1;
    return true;
}

bool Socket::ConnectBlocking( const char* addr, uint16_t port )
{
    m_sock = 1;
    return true;
}

void Socket::Close()
{
    m_sock = -1;
}

int Socket::Send( const void* _buf, int len )
{
    tracy_rtt_write_blocking(_buf, len);
    return len;
}

int Socket::GetSendBufSize()
{
    return tracy_rtt_get_max_write_size();
}

int Socket::ReadUpTo( void* _buf, int len )
{
    return tracy_rtt_read_nonblocking(_buf, len);
}

bool Socket::Read( void* buf, int len, int timeout )
{
    return tracy_rtt_read_all_or_nothing_blocking(buf, len, timeout);
}

bool Socket::ReadImpl( char*& buf, int& len, int timeout )
{
    if (tracy_rtt_read_all_or_nothing_blocking(buf, len, timeout)) {
        buf += len;
        len = 0;
        return true;
    } else {
        return false;
    }
}

bool Socket::ReadRaw( void* _buf, int len, int timeout )
{
    return tracy_rtt_read_blocking(_buf, len, timeout);
}

bool Socket::HasData()
{
    return tracy_rtt_has_data();
}

bool Socket::IsValid() const
{
    return m_sock.load( std::memory_order_relaxed ) >= 0;
}

}
