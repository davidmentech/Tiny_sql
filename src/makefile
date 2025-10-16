CXX = g++
CXXFLAGS = -Wall -std=c++20 -fdiagnostics-color=always -g  

TARGET = main
SRC = main.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)
