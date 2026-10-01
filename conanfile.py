from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps, cmake_layout


class PookieCppConan(ConanFile):
    name = "pookiecpp"
    version = "0.1.0"
    package_type = "static-library"
    settings = "os", "compiler", "build_type", "arch"
    exports_sources = "CMakeLists.txt", "cpp/*", "pookiepy/message.proto"

    def requirements(self):
        self.requires("grpc/1.72.0", transitive_headers=True, transitive_libs=True)
        self.requires("protobuf/5.27.0", transitive_headers=True, transitive_libs=True)

    def layout(self):
        cmake_layout(self)

    def generate(self):
        CMakeDeps(self).generate()
        toolchain = CMakeToolchain(self)
        toolchain.cache_variables["BUILD_TESTING"] = True
        toolchain.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        cmake.test()

    def package(self):
        CMake(self).install()

    def package_info(self):
        self.cpp_info.libs = ["pookiecpp"]
        self.cpp_info.requires = ["grpc::grpc++", "protobuf::libprotobuf"]