package main

import (
	"flag"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

const (
	durationOneYear   = 31536000
	durationThreeYear = 94608000
)

func getAWSOffering(svc *ec2.EC2, offering *ec2.ReservedInstancesOffering) (*ec2.ReservedInstancesOffering, error) {
	duration := *offering.Duration

	if duration <= durationOneYear {
		duration = durationOneYear
	} else {
		duration = durationThreeYear
	}

	output, err := svc.DescribeReservedInstancesOfferings(&ec2.DescribeReservedInstancesOfferingsInput{
		ProductDescription: offering.ProductDescription,
		IncludeMarketplace: aws.Bool(false),
		InstanceType:       offering.InstanceType,
		InstanceTenancy:    offering.InstanceTenancy,
		AvailabilityZone:   offering.AvailabilityZone,
		OfferingClass:      offering.OfferingClass,
		OfferingType:       offering.OfferingType,
		MinDuration:        &duration,
		MaxDuration:        &duration,
	})
	if err != nil {
		return nil, err
	}

	if found := len(output.ReservedInstancesOfferings); found > 2 {
		log.Printf("%#v", output.ReservedInstancesOfferings)
		return nil, fmt.Errorf("Expected 1 offering, found %d", found)
	} else if found == 2 && !reflect.DeepEqual(output.ReservedInstancesOfferings[0],
		output.ReservedInstancesOfferings[1]) {
		return nil, fmt.Errorf("Expected 1 offering, found %d", found)
	} else if found == 0 {
		return nil, fmt.Errorf("No AWS offering found for %s", getAssetName(offering))
	}

	return output.ReservedInstancesOfferings[0], nil
}

func getAssetName(offering *ec2.ReservedInstancesOffering) string {
	duration := "1yr"

	if *offering.Duration == durationThreeYear {
		duration = "3yr"
	}

	return fmt.Sprintf("%s:%s:%s:%s:%s.tenancy:%s.offering",
		*offering.InstanceType,
		duration,
		strings.Replace(strings.ToLower(*offering.ProductDescription), " ", ".", -1),
		strings.Replace(strings.ToLower(*offering.OfferingType), " ", ".", -1),
		*offering.InstanceTenancy,
		*offering.OfferingClass,
	)
}

func pageInstanceOfferings(svc *ec2.EC2, f func(offering *ec2.ReservedInstancesOffering) error) (n int, err error) {
	var ids = map[string]struct{}{}

	params := &ec2.DescribeReservedInstancesOfferingsInput{
		ProductDescription: aws.String("Linux/UNIX (Amazon VPC)"),
		IncludeMarketplace: aws.Bool(true),
		Filters: []*ec2.Filter{{
			Name: aws.String("marketplace"),
			Values: []*string{
				aws.String("true"),
			},
		}},
	}

	for {
		output, err := svc.DescribeReservedInstancesOfferings(params)
		if err != nil {
			return n, err
		}

		for _, offering := range output.ReservedInstancesOfferings {
			// the api seems to return duplicates, so we can stop when we see them
			if _, seen := ids[*offering.ReservedInstancesOfferingId]; seen {
				return n, err
			}
			if err = f(offering); err != nil {
				return n, err
			}
			n++
			ids[*offering.ReservedInstancesOfferingId] = struct{}{}
		}

		if output.NextToken == nil {
			break
		}

		params.NextToken = output.NextToken
	}

	return n, err
}

func main() {
	debugFlag := flag.Bool("debug", false, "Show debugging output")
	flag.Parse()

	config := aws.NewConfig()
	if *debugFlag {
		config = config.WithLogLevel(
			aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors | aws.LogDebugWithHTTPBody,
		)
	}

	sess, err := session.NewSession(config.WithRegion("us-east-1"))
	if err != nil {
		fmt.Println("failed to create session,", err)
		return
	}

	svc := ec2.New(sess)

	db, err := initDatabase()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Finding reserved instance offerings")

	n, err := pageInstanceOfferings(svc, func(offering *ec2.ReservedInstancesOffering) error {
		_, err := getAWSOffering(svc, offering)
		if err != nil {
			log.Printf("Error getting offering: %v", err)
			return nil
		}
		log.Printf("Storing %s (%s)", *offering.ReservedInstancesOfferingId, getAssetName(offering))
		return db.StoreOffering(offering)
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Found %d offerings", n)
}
