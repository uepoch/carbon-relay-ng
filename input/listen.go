package input

import (
	"bytes"
	"net"
	"time"

	"github.com/jpillora/backoff"
)

func listen(addr string, handler Handler) error {
	// listeners are set up outside of accept* here so they can interrupt startup
	listener, err := listenTcp(addr)
	if err != nil {
		return err
	}

	udp_conn, err := listenUdp(addr)
	if err != nil {
		return err
	}

	go acceptTcp(addr, listener, handler)
	go acceptUdp(addr, udp_conn, handler)

	return nil
}

func listenTcp(addr string) (*net.TCPListener, error) {
	laddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	return net.ListenTCP("tcp", laddr)
}

func acceptTcp(addr string, l *net.TCPListener, handler Handler) {
	var err error
	var backoffTimer *time.Timer
	backoffCounter := &backoff.Backoff{
		Min: 500 * time.Millisecond,
		Max: time.Minute,
	}

	socketWg.Add(1)
	defer socketWg.Done()

	go func() {
		<-shutdown
		l.Close()
	}()

	for {
		log.Notice("listening on %v/tcp", addr)

	ACCEPT:
		for {
			c, err := l.AcceptTCP()
			if err != nil {
				select {
				// socket has been closed due to shut down, log and return
				case <-shutdown:
					log.Info("shutting down %v/tcp, closing socket", addr)
					return
				default:
					log.Error("error accepting on %v/tcp, closing connection: %s", addr, err)
					l.Close()
					break ACCEPT
				}
			}

			// handle the connection
			log.Debug("listen.go: tcp connection from %v", c.RemoteAddr())
			go acceptTcpConn(c, handler)
		}

		for {
			log.Notice("reopening %v/tcp", addr)
			l, err = listenTcp(addr)
			if err == nil {
				backoffCounter.Reset()
				break
			}

			backoffDuration := backoffCounter.Duration()
			log.Error("error listening on %v/tcp, retrying after %v: %s", addr, backoffDuration, err)
			backoffTimer = time.NewTimer(backoffDuration)

			// continue looping once the backoffTimer triggers,
			// unless a shutdown event gets triggered first
			select {
			case <-shutdown:
				backoffTimer.Stop()
				log.Info("shutting down %v/tcp, closing socket", addr)
				return
			case <-backoffTimer.C:
			}
		}
	}
}

func acceptTcpConn(c net.Conn, handler Handler) {
	socketWg.Add(1)
	defer socketWg.Done()

	go func() {
		<-shutdown
		c.Close()
	}()

	handler.Handle(c)
}

func listenUdp(addr string) (*net.UDPConn, error) {
	udp_addr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return net.ListenUDP("udp", udp_addr)
}

func acceptUdp(addr string, l *net.UDPConn, handler Handler) {
	var err error
	buffer := make([]byte, 65535)
	var backoffTimer *time.Timer
	backoffCounter := &backoff.Backoff{
		Min: 500 * time.Millisecond,
		Max: time.Minute,
	}

	socketWg.Add(1)
	defer socketWg.Done()

	go func() {
		<-shutdown
		l.Close()
	}()

	for {
		log.Notice("listening on %v/udp", addr)

	READ:
		for {
			// read a packet into buffer
			b, src, err := l.ReadFrom(buffer)
			if err != nil {
				select {
				// socket has been closed due to shut down, log and return
				case <-shutdown:
					log.Info("shutting down %v/udp, closing socket", addr)
					return
				default:
					log.Error("error reading packet on %v/udp, closing connection: %s", addr, err)
					l.Close()
					break READ
				}
			}

			// handle the packet
			log.Debug("listen.go: udp packet from %v (length: %d)", src, b)
			handler.Handle(bytes.NewReader(buffer[:b]))
		}

		for {
			log.Notice("reopening %v/udp", addr)

			l, err = listenUdp(addr)
			if err == nil {
				backoffCounter.Reset()
				break
			}

			backoffDuration := backoffCounter.Duration()

			log.Error("error listening on %v/udp, retrying after %v: %s", addr, backoffDuration, err)
			backoffTimer = time.NewTimer(backoffDuration)

			// continue looping once the backoffTimer triggers,
			// unless a shutdown event gets triggered first
			select {
			case <-shutdown:
				backoffTimer.Stop()
				log.Info("shutting down %v/udp, closing socket", addr)
				return
			case <-backoffTimer.C:
			}
		}
	}
}
