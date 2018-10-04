// **********************************************************************
//
// Copyright (c) 2018-present ZeroC, Inc. All rights reserved.
//
// **********************************************************************

#pragma once

#include <DataStorm/Config.h>
#include <DataStorm/InternalI.h>

#ifndef DATASTORM_API
#   ifdef DATASTORM_API_EXPORTS
#       define DATASTORM_API DATASTORM_DECLSPEC_EXPORT
#   elif defined(DATASTORM_STATIC_LIBS)
#       define DATASTORM_API /**/
#   else
#       define DATASTORM_API DATASTORM_DECLSPEC_IMPORT
#   endif
#endif

namespace DataStorm
{

template<typename, typename, typename> class Topic;

/**
 * The Node class allows creating topic readers and writers.
 *
 * A Node is the main DataStorm object which allows creating topic readers or writers.
 */
class DATASTORM_API Node
{
public:

    /**
     * Construct a DataStorm node.
     *
     * A node is the main DataStorm object. It is required to construct topics.
     * The node uses the given Ice communicator.
     *
     * @param communicator The Ice communicator used by the topic factory for
     *                     its configuration and communications.
     */
    Node(std::shared_ptr<Ice::Communicator> communicator);

    /**
     * Construct a DataStorm node.
     *
     * A node is the main DataStorm object. It is required to construct topics.
     * This constructor parses the command line arguments into Ice properties and
     * initialize a new Node. The constructor initializes the Ice communicator
     * using the given Ice arguments. If the communicator creation fails, an Ice
     * exception is raised.
     *
     * @param argc The number of command line arguments in the argv array.
     * @param argv The command line arguments.
     * @param iceArgs Additonal arguments which are passed to the Ice::initialize
     *                function in addition to the argc and argv arguments.
     */
    template<class... T>
    Node(int& argc, const char* argv[], T&&... iceArgs) : _ownsCommunicator(true)
    {
        init(argc, argv, std::forward<T>(iceArgs)...);
    }

    /**
     * Construct a DataStorm node.
     *
     * A node is the main DataStorm object. It is required to construct topics.
     * This constructor parses the command line arguments into Ice properties and
     * initialize a new Node. The constructor initializes the Ice communicator
     * using the given Ice arguments. If the communicator creation fails, an Ice
     * exception is raised.
     *
     * @param argc The number of command line arguments in the argv array.
     * @param argv The command line arguments.
     * @param iceArgs Additonal arguments which are passed to the Ice::initialize
     *                function in addition to the argc and argv arguments.
     */
    template<class... T>
    Node(int& argc, char* argv[], T&&... iceArgs) : _ownsCommunicator(true)
    {
        init(argc, argv, std::forward<T>(iceArgs)...);
    }

#ifdef _WIN32
    /**
     * Construct a DataStorm node.
     *
     * A node is the main DataStorm object. It is required to construct topics.
     * This constructor parses the command line arguments into Ice properties and
     * initialize a new Node. The constructor initializes the Ice communicator
     * using the given Ice arguments. If the communicator creation fails, an Ice
     * exception is raised.
     *
     * @param argc The number of command line arguments in the argv array.
     * @param argv The command line arguments.
     * @param iceArgs Additonal arguments which are passed to the Ice::initialize
     *                function in addition to the argc and argv arguments.
     */
    template<class... T>
    Node(int& argc, const wchar_t* argv[], T&&... iceArgs) : _ownsCommunicator(true)
    {
        init(argc, argv, std::forward<T>(iceArgs)...);
    }

    /**
     * Construct a DataStorm node.
     *
     * A node is the main DataStorm object. It is required to construct topics.
     * This constructor parses the command line arguments into Ice properties and
     * initialize a new Node. The constructor initializes the Ice communicator
     * using the given Ice arguments. If the communicator creation fails, an Ice
     * exception is raised.
     *
     * @param argc The number of command line arguments in the argv array.
     * @param argv The command line arguments.
     * @param iceArgs Additonal arguments which are passed to the Ice::initialize
     *                function in addition to the argc and argv arguments.
     */
    template<class... T>
    Node(int& argc, wchar_t* argv[], T&&... iceArgs) : _ownsCommunicator(true)
    {
        init(argc, argv, std::forward<T>(iceArgs)...);
    }
#endif

    /**
     * Construct a DataStorm node.
     *
     * A node is the main DataStorm object. It is required to construct topics.
     * The constructor initializes the Ice communicator using the given arguments.
     * If the communicator creation fails, an Ice exception is raised.
     *
     * @param iceArgs Arguments which are passed to the Ice::initialize function.
     */
    template<class... T>
    Node(T&&... iceArgs) : _ownsCommunicator(true)
    {
        init(Ice::initialize(std::forward<T>(iceArgs)...));
    }

    /**
     * Construct a new Node by taking ownership of the given node.
     *
     * @param node The node to transfer ownership from.
     */
    Node(Node&& node);

    /**
     * Node destructor. The node destruction releases associated resources.
     * If the node created the Ice communicator, the communicator is destroyed.
     */
    ~Node();

    /**
     * Shutdown the node. The shutdown interrupts calls which are waiting for
     * events, writers or readers.
     **/
    void shutdown();

    /**
     * Return whether or not the node shutdown has been initiated.
     *
     * @return True if the node is shutdown, false otherwise.
     */
    bool isShutdown();

    /**
     * Shutdown the node when Ctrl-C is handled.
     *
     * @param handler The Ctrl-C handler to use for setting up the shutdown
     * callback.
     */
    void shutdownOnCtrlC(Ice::CtrlCHandler& handler) const;

    /**
     * Move assignement operator.
     *
     * @param node The node.
     **/
    Node& operator=(Node&& node);

    /**
     * Returns the Ice communicator associated with the node.
     */
    std::shared_ptr<Ice::Communicator> getCommunicator() const;

    /**
     * Returns the Ice connection associated with a session given a session
     * identifir. Session identifiers are provided with the sample origin
     * data member as the first tuple element.
     *
     * @param ident The session identifier.
     * @return The connection associated with the given session
     * @see DataStorm::Sample::ElementId DataStorm::Sample::getSession
     */
    std::shared_ptr<Ice::Connection> getSessionConnection(const std::string& ident) const;

private:

    template<typename V, class... T> void init(int& argc, V argv, T&&... iceArgs)
    {
        auto communicator = Ice::initialize(argc, argv, std::forward<T>(iceArgs)...);
        auto args = Ice::argsToStringSeq(argc, argv);
        communicator->getProperties()->parseCommandLineOptions("DataStorm", args);
        Ice::stringSeqToArgs(args, argc, argv);
        init(communicator);
    }

    void init(const std::shared_ptr<Ice::Communicator>&);

    std::shared_ptr<DataStormI::Instance> _instance;
    std::shared_ptr<DataStormI::TopicFactory> _factory;
    bool _ownsCommunicator;

    template<typename, typename, typename> friend class Topic;
};

}
