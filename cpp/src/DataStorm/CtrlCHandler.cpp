// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <DataStorm/CtrlCHandler.h>
#include <mutex>
#include <cassert>

#ifdef _WIN32
#   include <condition_variable>
#   include <windows.h>
#else
#   include <signal.h>
#endif

using namespace DataStorm;

namespace
{

std::mutex _mutex;
CtrlCHandlerCallback _callback;
CtrlCHandler* _handler = nullptr;
bool _signalsMasked = false;

}

CtrlCHandlerCallback
CtrlCHandler::setCallback(CtrlCHandlerCallback callback)
{
    std::lock_guard<std::mutex> lg(_mutex);
    CtrlCHandlerCallback oldCallback = _callback;
    _callback = callback;
    return oldCallback;
}

CtrlCHandlerCallback
CtrlCHandler::getCallback() const
{
    std::lock_guard<std::mutex> lg(_mutex);
    return _callback;
}

#ifdef _WIN32

namespace
{

int _inProgress = 0;
std::condition_variable* _cond = nullptr;

}

static BOOL WINAPI handlerRoutine(DWORD dwCtrlType)
{
    CtrlCHandlerCallback callback;
    {
        std::lock_guard<std::mutex> lg(_mutex);
        if(!_callback) // No callback set
        {
            return TRUE;
        }
        callback = _callback;
        ++_inProgress;
    }
    assert(callback);
    callback(dwCtrlType);
    {
        std::lock_guard<std::mutex> lg(_mutex);
        --_inProgress;
        if(_inProgress == 0 && _cond)
        {
            _cond->notify_one();
        }
    }
    return TRUE;
}

void
CtrlCHandler::maskSignals() noexcept
{
    std::lock_guard<std::mutex> lg(_mutex);
    if(!_signalsMasked)
    {
        SetConsoleCtrlHandler(handlerRoutine, TRUE);
        _signalsMasked = true;
    }
}

CtrlCHandler::CtrlCHandler(CtrlCHandlerCallback callback)
{
    maskSignals();

    std::lock_guard<std::mutex> lg(_mutex);
    if(_handler)
    {
        throw std::logic_error("you cannot create more than one CtrlCHandler at a time");
    }

    _callback = callback;
    _handler = this;
}

CtrlCHandler::~CtrlCHandler()
{
    std::unique_lock<std::mutex> lk(_mutex);
    _handler = nullptr;
    _callback = nullptr;
    if(_inProgress > 0)
    {
        std::condition_variable cond;
        _cond = &cond;
        cond.wait(lk, []{ return _inProgress == 0; });
    }
}

#else

extern "C"
{

static void*
sigwaitThread(void*)
{
    sigset_t ctrlCLikeSignals;
    sigemptyset(&ctrlCLikeSignals);
    sigaddset(&ctrlCLikeSignals, SIGHUP);
    sigaddset(&ctrlCLikeSignals, SIGINT);
    sigaddset(&ctrlCLikeSignals, SIGTERM);

    //
    // Run until the handler is destroyed (_handler == nullptr)
    //
    for(;;)
    {
        int signal = 0;
        int rc = sigwait(&ctrlCLikeSignals, &signal);
        if(rc == EINTR)
        {
            //
            // Some sigwait() implementations incorrectly return EINTR
            // when interrupted by an unblocked caught signal
            //
            continue;
        }
        assert(rc == 0);

        CtrlCHandlerCallback callback;
        {
            std::lock_guard<std::mutex> lg(_mutex);
            if(!_handler) // The handler is destroyed.
            {
                break;
            }
            callback = _callback;
        }

        if(callback)
        {
            callback(signal);
        }
    }
    return 0;
}

}

namespace
{

pthread_t _tid;

}

void
CtrlCHandler::maskSignals() noexcept
{
    std::lock_guard<std::mutex> lg(_mutex);
    if(!_signalsMasked)
    {
        //
        // We block these CTRL+C like signals in the main thread, and by default all other
        // threads will inherit this signal mask.
        //
        sigset_t ctrlCLikeSignals;
        sigemptyset(&ctrlCLikeSignals);
        sigaddset(&ctrlCLikeSignals, SIGHUP);
        sigaddset(&ctrlCLikeSignals, SIGINT);
        sigaddset(&ctrlCLikeSignals, SIGTERM);

#ifndef NDEBUG
        int rc = pthread_sigmask(SIG_BLOCK, &ctrlCLikeSignals, 0);
        assert(rc == 0);
#else
        pthread_sigmask(SIG_BLOCK, &ctrlCLikeSignals, 0);
#endif
        _signalsMasked = true;
    }
}

CtrlCHandler::CtrlCHandler(CtrlCHandlerCallback callback)
{
    maskSignals();

    {
        std::lock_guard<std::mutex> lg(_mutex);
        if(_handler)
        {
            throw std::logic_error("you cannot create more than one CtrlCHandler at a time");
        }
        _callback = callback;
        _handler = this;
    }

    // Joinable thread
#ifndef NDEBUG
    int rc = pthread_create(&_tid, 0, sigwaitThread, 0);
    assert(rc == 0);
#else
    pthread_create(&_tid, 0, sigwaitThread, 0);
#endif
}

CtrlCHandler::~CtrlCHandler()
{
    //
    // Clear the handler, the sigwaitThread will exit if _handler is null
    //
    {
        std::lock_guard<std::mutex> lg(_mutex);
        _handler = nullptr;
        _callback = nullptr;
    }

    //
    // Signal the sigwaitThread and join it.
    //
    void* status = nullptr;
#ifndef NDEBUG
    int rc = pthread_kill(_tid, SIGTERM);
    assert(rc == 0);
    rc = pthread_join(_tid, &status);
    assert(rc == 0);
#else
    pthread_kill(_tid, SIGTERM);
    pthread_join(_tid, &status);
#endif
}

#endif
