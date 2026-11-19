// Calls into the probe DLL, so the DLL unloads only at process exit.

extern "C" __declspec(dllimport) int connectAndClose(const char* url);

int main(int argc, char* argv[]) {
    if (argc != 2)
        return 2;
    return connectAndClose(argv[1]);
}
