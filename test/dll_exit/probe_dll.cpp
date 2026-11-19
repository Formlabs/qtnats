// Uses qtnats from a DLL the way a Python extension does, so test_core can check that the process exits.

#include <qtnats/qtnats.h>

#include <memory>

#include <QCoreApplication>
#include <QEventLoop>
#include <QTimer>

using namespace QtNats;

extern "C" __declspec(dllexport) int connectAndClose(const char* url) {
    int argc = 1;
    char name[] = "qtnats_exit_probe";
    char* argv[] = {name, nullptr};
    auto app = std::make_unique<QCoreApplication>(argc, argv);
    auto client = std::make_unique<Client>();

    QEventLoop loop;
    QObject::connect(client.get(), &Client::statusChanged, &loop, [&loop](ConnectionStatus s) {
        if (s == ConnectionStatus::Connected)
            loop.quit();
    });
    QTimer::singleShot(5000, &loop, &QEventLoop::quit);
    client->connectToServerAsync(QUrl(QString::fromUtf8(url)));
    loop.exec();
    if (client->status() != ConnectionStatus::Connected)
        return 1;

    // Python can release the application before the object that closes the Client, which restarts the pool.
    app.reset();
    client->closeAsync();
    client.reset();
    return 0;
}
