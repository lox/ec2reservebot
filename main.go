package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

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
	config := aws.NewConfig()
	// config = config.WithLogLevel(
	// 	aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors | aws.LogDebugWithHTTPBody,
	// )

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
		log.Printf("Storing %s", *offering.ReservedInstancesOfferingId)
		return db.StoreOffering(offering)
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Found %d offerings", n)
}
