
// TODO temporary until we fix https://github.com/zeroc-ice/ice/issues/2877

#ifndef DATASTORMI_TIMER_TASKI_H
#define DATASTORMI_TIMER_TASKI_H

#include "Ice/Timer.h"

#include <functional>

namespace DataStormI
{
    class TimerTaskI : public Ice::TimerTask
    {
    public:
        TimerTaskI(std::function<void()> task) : _task(std::move(task)) {}

        void runTimerTask() override { _task(); }

    private:
        std::function<void()> _task;
    };
}

#endif
