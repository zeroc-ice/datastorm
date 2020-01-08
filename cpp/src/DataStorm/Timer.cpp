// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#include <vector>
#include <assert.h>

#include <DataStorm/Timer.h>

using namespace std;
using namespace DataStormI;

Timer::Timer() :
    _destroyed(false)
{
    _thread = thread(&Timer::runTimer, this);
}

function<void()>
Timer::schedule(chrono::milliseconds duration, function<void()> callback)
{
    lock_guard<mutex> lock(_mutex);
    auto scheduledAt = chrono::steady_clock::now() + duration;
    auto it = _timers.emplace(scheduledAt, callback);
    _cond.notify_one();
    return [=, self=shared_from_this()]() {
        lock_guard<mutex> lock(_mutex);
        if(scheduledAt > chrono::steady_clock::now())
        {
            _timers.erase(it);
        }
    };
}

void
Timer::destroy()
{
    unique_lock<mutex> lock(_mutex);
    _destroyed = true;
    _timers.clear(); // TODO: Notify the timers instead?
    _cond.notify_one();
    lock.unlock();
    _thread.join();
}

void
Timer::runTimer()
{
    vector<function<void()>> tasks;
    while(true)
    {
        {
            unique_lock<mutex> lock(_mutex);
            if(_destroyed)
            {
                return;
            }

            if(_timers.empty())
            {
                _cond.wait(lock, [this] { return !_timers.empty() || _destroyed; });
            }
            else
            {
                _cond.wait_until(lock,
                                 _timers.cbegin()->first,
                                 [=] { return _timers.cbegin()->first <= chrono::steady_clock::now() || _destroyed; });
                auto now = chrono::steady_clock::now();
                auto p = _timers.begin();
                for(; p != _timers.end() && p->first <= now; ++p)
                {
                    tasks.push_back(move(p->second));
                }
                _timers.erase(_timers.begin(), p);
            }
        }
        if(!tasks.empty())
        {
            for(auto& t : tasks)
            {
                try
                {
                    t();
                }
                catch(...)
                {
                    assert(false);
                }
            }
        }
        tasks.clear();
    }
}
