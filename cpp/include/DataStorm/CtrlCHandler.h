// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#ifndef DATASTORM_CTRL_C_HANDLER_H
#define DATASTORM_CTRL_C_HANDLER_H

#include <DataStorm/Config.h>
#include <functional>

namespace DataStorm
{

/**
 * Invoked when a signal occurs. The callback must not raise exceptions.
 * On Linux and macOS, the callback is NOT a signal handler and can call
 * functions that are not async-signal safe.
 * @param sig The signal number that occurred.
 */
using CtrlCHandlerCallback = std::function<void(int sig)>;

/**
 * Provides a portable way to handle Ctrl-C and Ctrl-C like signals.
 * On Linux and macOS, the CtrlCHandler handles SIGHUP, SIGINT and SIGTERM.
 * On Windows, it is essentially a wrapper for SetConsoleCtrlHandler().
 *
 * \headerfile DataStorm/CtrlCHandler.h
 */
class DATASTORM_API CtrlCHandler
{
public:

    /**
     * Linux and macOS: mask the SIGHUP, SIGINT and SIGTERM signals. It is
     * essential to call maskSignals before creating any thread. Threads
     * created later on will inherit this signal mask.
     * Windows: call SetConsoleCtrlCHandler to register a handler routine that
     * ignores signals.
     */
    static void maskSignals() noexcept;

    /**
     * Register a function that handles Ctrl-C like signals.
     * This constructor first calls maskSignals if it was not already called.
     * On Linux and macOS, it creates a thread that waits on SIGHUP, SIGINT and
     * SIGTERM using sigwait.
     * Only a single CtrlCHandler object can exist in a process at a give time.
     *
     * @param cb The callback function to invoke when a signal is received.
     */
    explicit CtrlCHandler(CtrlCHandlerCallback cb = nullptr);

    /**
     * Unregister the callback function.
     * This destructor does not "unmask" SIGHUP, SIGINT and SIGTERM or
     * unregister the handler routine on Windows. As a result, Ctrl-C and
     * similar signals are just ignored after this destructor completes.
     */
    ~CtrlCHandler();

    /**
     * Replace the signal callback.
     * @param cb The new callback.
     * @return The old callback
     */
    CtrlCHandlerCallback setCallback(CtrlCHandlerCallback cb);

    /**
     * Obtain the signal callback.
     * @return The callback
     */
    CtrlCHandlerCallback getCallback() const;
};
}

#endif
