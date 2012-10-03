using System;
class Program
{
	static void Main()
	{
		// To match linux date format: %Y-%m-%d-%H-%M-%S-%N
		Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd-hh-mm-ss-ffffff"));
	}
}

